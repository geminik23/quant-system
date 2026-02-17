//! Unified TUI client for the market data service.
//!
//! Combines live price streaming with an interactive command palette
//! in a single tabbed terminal interface.
//!
//! Usage:
//!   cargo run -p market-data --features tui-client --bin market_data_client
//!   cargo run -p market-data --features tui-client --bin market_data_client -- --shm-name market-data --symbols eurusd,xauusd

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::Parser;
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind};
use futures::StreamExt;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Cell, List, ListItem, ListState, Paragraph, Row, Table, TableState, Tabs,
    Wrap,
};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc;

use market_data::rpc_types::*;
use xrpc::{MessageChannelAdapter, RpcClient, SharedMemoryConfig, SharedMemoryFrameTransport};

// ── Types ──

type MarketClient = RpcClient<MessageChannelAdapter<SharedMemoryFrameTransport>>;

#[derive(Parser, Debug)]
#[command(
    name = "market-data-client",
    about = "Market data TUI client — live prices + command palette"
)]
struct Args {
    /// Shared memory acceptor name (must match server config)
    #[arg(long, default_value = "market-data")]
    shm_name: String,

    /// Symbols to subscribe (comma-separated, empty = all)
    #[arg(long, default_value = "")]
    symbols: String,

    /// Client name sent during handshake
    #[arg(long, default_value = "tui-client")]
    client_name: String,
}

// ── Connection ──

async fn connect(
    shm_name: &str,
    client_name: &str,
) -> Result<(MarketClient, ConnectResponse), Box<dyn std::error::Error>> {
    let accept_name = format!("{}-accept", shm_name);

    let acceptor_transport = SharedMemoryFrameTransport::connect_client(&accept_name)?;
    let acceptor_channel = MessageChannelAdapter::new(acceptor_transport);
    let acceptor_client = RpcClient::new(acceptor_channel);
    let _handle = acceptor_client.start();

    let resp: ConnectResponse = acceptor_client
        .call(
            "connect",
            &ConnectRequest {
                client_name: client_name.into(),
            },
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(50)).await;
    acceptor_client.close().await?;

    let cfg = SharedMemoryConfig::new()
        .with_read_timeout(Duration::from_secs(300))
        .with_write_timeout(Duration::from_secs(30));
    let transport = SharedMemoryFrameTransport::connect_client_with_config(&resp.slot_name, cfg)?;
    let channel = MessageChannelAdapter::new(transport);
    let client = RpcClient::new(channel);
    let _handle = client.start();

    Ok((client, resp))
}

// ── Helpers ──

fn format_ts(ts_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .map(|dt| dt.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| ts_ms.to_string())
}

// ── Menu Items ──

#[derive(Clone, Copy, PartialEq)]
enum MenuItem {
    Ping,
    GetState,
    GetSymbols,
    GetPrice,
    GetPrices,
    SetAlert,
    RemoveAlert,
    GetAlerts,
}

impl MenuItem {
    const ALL: &[Self] = &[
        Self::Ping,
        Self::GetState,
        Self::GetSymbols,
        Self::GetPrice,
        Self::GetPrices,
        Self::SetAlert,
        Self::RemoveAlert,
        Self::GetAlerts,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Ping => "Ping",
            Self::GetState => "Get State",
            Self::GetSymbols => "Get Symbols",
            Self::GetPrice => "Get Price",
            Self::GetPrices => "Get Prices",
            Self::SetAlert => "Set Alert",
            Self::RemoveAlert => "Remove Alert",
            Self::GetAlerts => "Get Alerts",
        }
    }

    fn input_fields(self) -> &'static [&'static str] {
        match self {
            Self::GetPrice => &["Symbol"],
            Self::GetPrices => &["Symbols (comma-separated)"],
            Self::SetAlert => &["Symbol", "Kind (ABOVE/BELOW)", "Price"],
            Self::RemoveAlert => &["Alert ID"],
            _ => &[],
        }
    }
}

// ── RPC Execution ──

async fn execute_rpc(client: &MarketClient, item: MenuItem, inputs: Vec<String>) -> String {
    match item {
        MenuItem::Ping => match client.call::<_, CommandAck>("ping", &()).await {
            Ok(a) => format!("kind={}  ref={}", a.kind, a.reference),
            Err(e) => format!("ERROR: {e}"),
        },
        MenuItem::GetState => match client.call::<_, GetStateResponse>("get_state", &()).await {
            Ok(r) => format!("state={}  ts_ms={}", r.state, r.ts_ms),
            Err(e) => format!("ERROR: {e}"),
        },
        MenuItem::GetSymbols => {
            match client
                .call::<_, GetSymbolListResponse>("get_symbols", &())
                .await
            {
                Ok(r) => {
                    let count = r.symbols.len();
                    let list = if count <= 30 {
                        r.symbols.join(", ")
                    } else {
                        format!("{}, ... ({} more)", r.symbols[..30].join(", "), count - 30)
                    };
                    format!("{count} symbols: {list}")
                }
                Err(e) => format!("ERROR: {e}"),
            }
        }
        MenuItem::GetPrice => {
            let sym = inputs.first().cloned().unwrap_or_default();
            match client
                .call::<_, GetPriceResponse>(
                    "get_price",
                    &GetPriceRequest {
                        symbol: sym.clone(),
                    },
                )
                .await
            {
                Ok(r) if r.found => {
                    format!(
                        "{}  bid={:.5}  ask={:.5}  ts={}",
                        r.symbol, r.bid, r.ask, r.ts_ms
                    )
                }
                Ok(_) => format!("{sym}: not found"),
                Err(e) => format!("ERROR: {e}"),
            }
        }
        MenuItem::GetPrices => {
            let syms: Vec<String> = inputs
                .first()
                .unwrap_or(&String::new())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            match client
                .call::<_, GetPricesResponse>("get_prices", &GetPricesRequest { symbols: syms })
                .await
            {
                Ok(r) => r
                    .prices
                    .iter()
                    .map(|p| {
                        format!(
                            "  {}  bid={:.5}  ask={:.5}  found={}",
                            p.symbol, p.bid, p.ask, p.found
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
                Err(e) => format!("ERROR: {e}"),
            }
        }
        MenuItem::SetAlert => {
            let symbol = inputs.first().cloned().unwrap_or_default();
            let kind = inputs.get(1).cloned().unwrap_or_default().to_uppercase();
            let price: f64 = inputs.get(2).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            match client
                .call::<_, CommandAck>(
                    "set_alert",
                    &SetAlertRequest {
                        alert_id: String::new(),
                        symbol,
                        price,
                        kind,
                    },
                )
                .await
            {
                Ok(a) => format!("kind={}  alert_id={}", a.kind, a.reference),
                Err(e) => format!("ERROR: {e}"),
            }
        }
        MenuItem::RemoveAlert => {
            let id = inputs.first().cloned().unwrap_or_default();
            match client
                .call::<_, CommandAck>("remove_alert", &RemoveAlertRequest { alert_id: id })
                .await
            {
                Ok(a) => format!("kind={}  ref={}", a.kind, a.reference),
                Err(e) => format!("ERROR: {e}"),
            }
        }
        MenuItem::GetAlerts => match client.call::<_, GetAlertsResponse>("get_alerts", &()).await {
            Ok(r) if r.alerts.is_empty() => "No active alerts".to_string(),
            Ok(r) => r
                .alerts
                .iter()
                .map(|a| {
                    format!(
                        "  [{}] {} {} @ {:.5}",
                        a.alert_id, a.symbol, a.kind, a.price
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"),
            Err(e) => format!("ERROR: {e}"),
        },
    }
}

// ── UI State ──

#[derive(Clone, Copy, PartialEq)]
enum AppTab {
    Stream,
    Commands,
}

impl AppTab {
    fn index(self) -> usize {
        match self {
            Self::Stream => 0,
            Self::Commands => 1,
        }
    }
}

#[derive(Clone, PartialEq)]
enum CmdMode {
    Menu,
    Input { field_index: usize },
    Loading,
}

enum ConnStatus {
    Connecting,
    Connected,
    Error(String),
}

#[derive(Clone)]
struct PriceRow {
    symbol: String,
    bid: f64,
    ask: f64,
    spread: f64,
    last_update: String,
    flash: bool,
}

struct AlertRow {
    time: String,
    symbol: String,
    status: String,
    ref_price: f64,
    alert_id: String,
}

struct LogEntry {
    label: String,
    result: String,
}

struct App {
    // Connection
    client_id: usize,
    slot_name: String,
    conn_status: ConnStatus,
    subscribed: Vec<String>,

    // Tab
    active_tab: AppTab,

    // Stream state
    prices: Vec<PriceRow>,
    price_idx: HashMap<String, usize>,
    table_state: TableState,
    alerts: VecDeque<AlertRow>,
    tick_count: u64,
    last_update: String,

    // Command state
    selected_menu: usize,
    menu_state: ListState,
    cmd_mode: CmdMode,
    input_fields: Vec<(String, String)>, // (label, value)
    current_item: MenuItem,
    log: VecDeque<LogEntry>,
    log_scroll: u16,
    spinner: u8,

    // Common
    should_quit: bool,
    rpc_tx: mpsc::Sender<(String, String)>,
    rpc_rx: mpsc::Receiver<(String, String)>,
}

impl App {
    fn new(symbols: Vec<String>) -> Self {
        let (rpc_tx, rpc_rx) = mpsc::channel(4);
        Self {
            client_id: 0,
            slot_name: String::new(),
            conn_status: ConnStatus::Connecting,
            subscribed: symbols,

            active_tab: AppTab::Stream,

            prices: Vec::new(),
            price_idx: HashMap::new(),
            table_state: TableState::default(),
            alerts: VecDeque::new(),
            tick_count: 0,
            last_update: "--:--:--.---".into(),

            selected_menu: 0,
            menu_state: ListState::default().with_selected(Some(0)),
            cmd_mode: CmdMode::Menu,
            input_fields: Vec::new(),
            current_item: MenuItem::Ping,
            log: VecDeque::new(),
            log_scroll: 0,
            spinner: 0,

            should_quit: false,
            rpc_tx,
            rpc_rx,
        }
    }

    // ── Stream handlers ──

    fn on_price_tick(&mut self, tick: PriceTick) {
        let spread = tick.ask - tick.bid;
        let ts = format_ts(tick.ts_ms);
        let row = PriceRow {
            symbol: tick.symbol.clone(),
            bid: tick.bid,
            ask: tick.ask,
            spread,
            last_update: ts.clone(),
            flash: true,
        };
        if let Some(&i) = self.price_idx.get(&tick.symbol) {
            self.prices[i] = row;
        } else {
            let i = self.prices.len();
            self.price_idx.insert(tick.symbol.clone(), i);
            self.prices.push(row);
        }
        self.tick_count += 1;
        self.last_update = ts;
    }

    fn on_stream_event(&mut self, event: StreamEvent) {
        match event.event_type.as_str() {
            "PRICE" => {
                if let (Some(symbol), Some(bid), Some(ask)) = (event.symbol, event.bid, event.ask) {
                    self.on_price_tick(PriceTick {
                        symbol,
                        bid,
                        ask,
                        ts_ms: event.ts_ms,
                    });
                }
            }
            "STATE" => {
                if let Some(state_str) = &event.state {
                    self.update_conn_status(state_str);
                }
            }
            _ => {}
        }
    }

    fn update_conn_status(&mut self, state: &str) {
        self.conn_status = match state {
            "CONNECTED" => ConnStatus::Connected,
            "CONNECTING" | "LOGON" => ConnStatus::Connecting,
            "DISCONNECTED" => ConnStatus::Error("FIX DISCONNECTED".into()),
            other => ConnStatus::Error(format!("Unknown: {}", other)),
        };
    }

    fn on_alert(&mut self, alert: AlertResult) {
        self.alerts.push_front(AlertRow {
            time: format_ts(alert.ts_ms),
            symbol: alert.symbol,
            status: alert.status,
            ref_price: alert.ref_price,
            alert_id: alert.alert_id,
        });
        if self.alerts.len() > 50 {
            self.alerts.pop_back();
        }
    }

    fn clear_flash(&mut self) {
        for row in &mut self.prices {
            row.flash = false;
        }
    }

    // ── Command helpers ──

    fn selected_item(&self) -> MenuItem {
        MenuItem::ALL[self.selected_menu]
    }

    fn menu_up(&mut self) {
        if self.selected_menu > 0 {
            self.selected_menu -= 1;
        } else {
            self.selected_menu = MenuItem::ALL.len() - 1;
        }
        self.menu_state.select(Some(self.selected_menu));
    }

    fn menu_down(&mut self) {
        if self.selected_menu + 1 < MenuItem::ALL.len() {
            self.selected_menu += 1;
        } else {
            self.selected_menu = 0;
        }
        self.menu_state.select(Some(self.selected_menu));
    }

    fn enter_selected(&mut self) {
        let item = self.selected_item();
        self.current_item = item;
        let fields = item.input_fields();
        if fields.is_empty() {
            self.cmd_mode = CmdMode::Loading;
        } else {
            self.input_fields = fields
                .iter()
                .map(|&label| (label.to_string(), String::new()))
                .collect();
            self.cmd_mode = CmdMode::Input { field_index: 0 };
        }
    }

    fn collect_inputs(&self) -> Vec<String> {
        self.input_fields.iter().map(|(_, v)| v.clone()).collect()
    }

    fn push_log(&mut self, label: String, result: String) {
        self.log.push_back(LogEntry { label, result });
        if self.log.len() > 200 {
            self.log.pop_front();
        }
        self.log_scroll = 0;
    }
}

// ══════════════════════════════════════════════════════════
//  Rendering
// ══════════════════════════════════════════════════════════

fn render(f: &mut Frame, app: &mut App) {
    let [header_area, tab_bar_area, body_area, footer_area] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(1),
        Constraint::Min(8),
        Constraint::Length(1),
    ])
    .areas(f.area());

    render_header(f, app, header_area);
    render_tab_bar(f, app, tab_bar_area);

    match app.active_tab {
        AppTab::Stream => render_stream_tab(f, app, body_area),
        AppTab::Commands => render_commands_tab(f, app, body_area),
    }

    render_footer(f, app, footer_area);
}

// ── Header ──

fn render_header(f: &mut Frame, app: &App, area: Rect) {
    let status_style = match &app.conn_status {
        ConnStatus::Connected => Style::new().fg(Color::Green).bold(),
        ConnStatus::Connecting => Style::new().fg(Color::Yellow).bold(),
        ConnStatus::Error(_) => Style::new().fg(Color::Red).bold(),
    };
    let status_text = match &app.conn_status {
        ConnStatus::Connected => "CONNECTED".to_string(),
        ConnStatus::Connecting => "CONNECTING...".to_string(),
        ConnStatus::Error(e) => format!("ERROR: {}", e),
    };

    let sub_text = if app.subscribed.is_empty() {
        "all".to_string()
    } else {
        app.subscribed.join(", ")
    };

    let line1 = Line::from(vec![
        Span::styled(" Status: ", Style::new().bold()),
        Span::styled(&status_text, status_style),
        Span::raw("  "),
        Span::styled("Client: ", Style::new().bold()),
        Span::raw(app.client_id.to_string()),
        Span::raw("  "),
        Span::styled("Slot: ", Style::new().bold()),
        Span::styled(&app.slot_name, Style::new().fg(Color::DarkGray)),
    ]);

    let line2 = Line::from(vec![
        Span::styled(" Symbols: ", Style::new().bold()),
        Span::styled(&sub_text, Style::new().fg(Color::Cyan)),
        Span::raw("  "),
        Span::styled("Ticks: ", Style::new().bold()),
        Span::styled(
            format!("{}", app.tick_count),
            Style::new().fg(Color::Yellow),
        ),
        Span::raw("  "),
        Span::styled("Last: ", Style::new().bold()),
        Span::styled(&app.last_update, Style::new().fg(Color::DarkGray)),
    ]);

    let block = Block::bordered()
        .title(" Market Data Client ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Cyan));

    let paragraph = Paragraph::new(vec![line1, line2]).block(block);
    f.render_widget(paragraph, area);
}

// ── Tab Bar ──

fn render_tab_bar(f: &mut Frame, app: &App, area: Rect) {
    let titles = vec![" Stream ", " Commands "];
    let tabs = Tabs::new(titles)
        .select(app.active_tab.index())
        .style(Style::new().fg(Color::DarkGray))
        .highlight_style(
            Style::new()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        )
        .divider("│");
    f.render_widget(tabs, area);
}

// ── Stream Tab ──

fn render_stream_tab(f: &mut Frame, app: &mut App, area: Rect) {
    let [price_area, alert_area] =
        Layout::vertical([Constraint::Min(6), Constraint::Length(8)]).areas(area);

    render_price_table(f, app, price_area);
    render_alerts(f, app, alert_area);
}

fn render_price_table(f: &mut Frame, app: &mut App, area: Rect) {
    let header = Row::new(vec![
        Cell::from(" Symbol").style(Style::new().fg(Color::Yellow).bold()),
        Cell::from("Bid").style(Style::new().fg(Color::Yellow).bold()),
        Cell::from("Ask").style(Style::new().fg(Color::Yellow).bold()),
        Cell::from("Spread").style(Style::new().fg(Color::Yellow).bold()),
        Cell::from("Last Update").style(Style::new().fg(Color::Yellow).bold()),
    ])
    .height(1)
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .prices
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let bg = if i % 2 == 0 {
                Color::DarkGray
            } else {
                Color::Reset
            };
            let val_style = if p.flash {
                Style::new().fg(Color::Cyan).bg(bg)
            } else {
                Style::new().fg(Color::White).bg(bg)
            };
            Row::new(vec![
                Cell::from(format!(" {}", p.symbol))
                    .style(Style::new().fg(Color::Green).bold().bg(bg)),
                Cell::from(format!("{:.5}", p.bid)).style(val_style),
                Cell::from(format!("{:.5}", p.ask)).style(val_style),
                Cell::from(format!("{:.5}", p.spread))
                    .style(Style::new().fg(Color::DarkGray).bg(bg)),
                Cell::from(p.last_update.clone()).style(Style::new().fg(Color::DarkGray).bg(bg)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(12),
        Constraint::Fill(1),
    ];

    let block = Block::bordered()
        .title(" Live Prices ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Cyan));

    let table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .row_highlight_style(
            Style::new()
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");

    f.render_stateful_widget(table, area, &mut app.table_state);
}

fn render_alerts(f: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .alerts
        .iter()
        .map(|a| {
            let line = Line::from(vec![
                Span::styled(&a.time, Style::new().fg(Color::DarkGray)),
                Span::raw("  "),
                Span::styled(&a.symbol, Style::new().fg(Color::Green).bold()),
                Span::raw("  "),
                Span::styled(&a.status, Style::new().fg(Color::Yellow).bold()),
                Span::raw(format!("  {:.2}", a.ref_price)),
                Span::raw("  "),
                Span::styled(
                    format!("[{}]", &a.alert_id),
                    Style::new().fg(Color::DarkGray),
                ),
            ]);
            ListItem::new(line)
        })
        .collect();

    let block = Block::bordered()
        .title(" Triggered Alerts ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Cyan));

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

// ── Commands Tab ──

fn render_commands_tab(f: &mut Frame, app: &mut App, area: Rect) {
    let [menu_area, right_area] =
        Layout::horizontal([Constraint::Length(24), Constraint::Min(30)]).areas(area);

    render_menu(f, app, menu_area);
    render_right_panel(f, app, right_area);
}

fn render_menu(f: &mut Frame, app: &mut App, area: Rect) {
    let items: Vec<ListItem> = MenuItem::ALL
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let style = if i == app.selected_menu {
                Style::new()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD | Modifier::REVERSED)
            } else {
                Style::new().fg(Color::White)
            };

            let prefix = if i == app.selected_menu {
                " >> "
            } else {
                "    "
            };
            ListItem::new(format!("{}{}", prefix, item.label())).style(style)
        })
        .collect();

    let block = Block::bordered()
        .title(" Commands ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Magenta));

    let list = List::new(items).block(block);
    f.render_stateful_widget(list, area, &mut app.menu_state);
}

fn render_right_panel(f: &mut Frame, app: &mut App, area: Rect) {
    match &app.cmd_mode {
        CmdMode::Input { field_index } => {
            let field_count = app.input_fields.len();
            let input_height = (field_count as u16) + 5;
            let [input_area, log_area] = Layout::vertical([
                Constraint::Length(input_height.min(area.height / 2)),
                Constraint::Min(4),
            ])
            .areas(area);

            render_input_form(f, app, *field_index, input_area);
            render_log(f, app, log_area);
        }
        CmdMode::Loading => {
            let [loading_area, log_area] =
                Layout::vertical([Constraint::Length(3), Constraint::Min(4)]).areas(area);

            render_loading(f, app.spinner, loading_area);
            render_log(f, app, log_area);
        }
        CmdMode::Menu => {
            render_log(f, app, area);
        }
    }
}

fn render_input_form(f: &mut Frame, app: &App, active_field: usize, area: Rect) {
    let item = app.current_item;
    let mut lines = Vec::new();

    for (i, (label, value)) in app.input_fields.iter().enumerate() {
        let is_active = i == active_field;
        let label_style = if is_active {
            Style::new().fg(Color::Yellow).bold()
        } else {
            Style::new().fg(Color::DarkGray)
        };
        let value_style = if is_active {
            Style::new().fg(Color::White).bold()
        } else {
            Style::new().fg(Color::DarkGray)
        };
        let cursor = if is_active { "_" } else { "" };

        lines.push(Line::from(vec![
            Span::styled(format!("  {}: ", label), label_style),
            Span::styled(format!("{}{}", value, cursor), value_style),
        ]));
    }

    lines.push(Line::raw(""));
    lines.push(Line::from(vec![
        Span::raw("  "),
        Span::styled("[Enter]", Style::new().fg(Color::Yellow).bold()),
        Span::raw(" Next  "),
        Span::styled("[Esc]", Style::new().fg(Color::Yellow).bold()),
        Span::raw(" Cancel"),
    ]));

    let block = Block::bordered()
        .title(format!(" >> {} ", item.label()))
        .title_alignment(Alignment::Left)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Yellow));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn render_loading(f: &mut Frame, spinner: u8, area: Rect) {
    let frames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
    let ch = frames[(spinner as usize) % frames.len()];

    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Yellow));

    let text = Paragraph::new(format!("  {} Executing...", ch))
        .style(Style::new().fg(Color::Yellow).bold())
        .block(block);
    f.render_widget(text, area);
}

fn render_log(f: &mut Frame, app: &App, area: Rect) {
    let mut lines: Vec<Line> = Vec::new();

    for entry in app.log.iter().rev() {
        lines.push(Line::from(vec![Span::styled(
            format!(" [{}] ", entry.label),
            Style::new().fg(Color::Cyan).bold(),
        )]));
        for result_line in entry.result.lines() {
            lines.push(Line::from(vec![
                Span::raw("   "),
                Span::styled(result_line, Style::new().fg(Color::White)),
            ]));
        }
        lines.push(Line::raw(""));
    }

    let block = Block::bordered()
        .title(" Response Log ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Magenta));

    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false })
        .scroll((app.log_scroll, 0));
    f.render_widget(paragraph, area);
}

// ── Footer ──

fn render_footer(f: &mut Frame, app: &App, area: Rect) {
    let hints = match app.active_tab {
        AppTab::Stream => vec![
            Span::raw("  "),
            Span::styled("[Tab]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Commands  "),
            Span::styled("[↑↓]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Scroll  "),
            Span::styled("[q]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Quit"),
        ],
        AppTab::Commands => match &app.cmd_mode {
            CmdMode::Menu => vec![
                Span::raw("  "),
                Span::styled("[Tab]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Stream  "),
                Span::styled("[↑↓]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Navigate  "),
                Span::styled("[Enter]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Select  "),
                Span::styled("[PgUp/Dn]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Scroll log  "),
                Span::styled("[q]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Quit"),
            ],
            CmdMode::Input { .. } => vec![
                Span::raw("  "),
                Span::styled("[Enter]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Next field  "),
                Span::styled("[Shift+Enter]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Submit  "),
                Span::styled("[Esc]", Style::new().fg(Color::Yellow).bold()),
                Span::raw(" Cancel"),
            ],
            CmdMode::Loading => vec![
                Span::raw("  "),
                Span::styled("Executing RPC...", Style::new().fg(Color::Yellow)),
            ],
        },
    };
    f.render_widget(Paragraph::new(Line::from(hints)), area);
}

// ══════════════════════════════════════════════════════════
//  Event Handling
// ══════════════════════════════════════════════════════════

/// Returns `true` if an RPC should be fired (cmd_mode just changed to Loading).
fn handle_key(app: &mut App, code: KeyCode) -> bool {
    match app.active_tab {
        AppTab::Stream => handle_key_stream(app, code),
        AppTab::Commands => handle_key_commands(app, code),
    }
}

fn handle_key_stream(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Tab => app.active_tab = AppTab::Commands,
        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => app.should_quit = true,
        KeyCode::Up | KeyCode::Char('k') => {
            app.table_state.select_previous();
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.table_state.select_next();
        }
        KeyCode::Home | KeyCode::Char('g') => {
            app.table_state.select_first();
        }
        KeyCode::End | KeyCode::Char('G') => {
            app.table_state.select_last();
        }
        _ => {}
    }
    false
}

fn handle_key_commands(app: &mut App, code: KeyCode) -> bool {
    match &app.cmd_mode {
        CmdMode::Menu => handle_key_cmd_menu(app, code),
        CmdMode::Input { field_index } => {
            let fi = *field_index;
            handle_key_cmd_input(app, code, fi)
        }
        CmdMode::Loading => false, // ignore input while loading
    }
}

fn handle_key_cmd_menu(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Tab => {
            app.active_tab = AppTab::Stream;
        }
        KeyCode::Up | KeyCode::Char('k') => app.menu_up(),
        KeyCode::Down | KeyCode::Char('j') => app.menu_down(),
        KeyCode::Home | KeyCode::Char('g') => {
            app.selected_menu = 0;
            app.menu_state.select(Some(0));
        }
        KeyCode::End | KeyCode::Char('G') => {
            app.selected_menu = MenuItem::ALL.len() - 1;
            app.menu_state.select(Some(app.selected_menu));
        }
        KeyCode::Enter => app.enter_selected(),
        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => app.should_quit = true,
        KeyCode::PageUp => app.log_scroll = app.log_scroll.saturating_add(3),
        KeyCode::PageDown => app.log_scroll = app.log_scroll.saturating_sub(3),
        _ => {}
    }
    // Check if enter_selected changed mode to Loading (command with no input fields)
    app.cmd_mode == CmdMode::Loading
}

fn handle_key_cmd_input(app: &mut App, code: KeyCode, field_index: usize) -> bool {
    match code {
        KeyCode::Char(c) => {
            if let Some((_, val)) = app.input_fields.get_mut(field_index) {
                val.push(c);
            }
        }
        KeyCode::Backspace => {
            if let Some((_, val)) = app.input_fields.get_mut(field_index) {
                val.pop();
            }
        }
        KeyCode::Enter => {
            if field_index + 1 < app.input_fields.len() {
                app.cmd_mode = CmdMode::Input {
                    field_index: field_index + 1,
                };
            } else {
                // All fields filled — submit
                app.cmd_mode = CmdMode::Loading;
            }
        }
        KeyCode::Tab => {
            // Submit immediately regardless of which field we're on
            app.cmd_mode = CmdMode::Loading;
        }
        KeyCode::Esc => {
            app.input_fields.clear();
            app.cmd_mode = CmdMode::Menu;
        }
        _ => {}
    }
    app.cmd_mode == CmdMode::Loading
}

// ══════════════════════════════════════════════════════════
//  Main Loop
// ══════════════════════════════════════════════════════════

async fn run(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    client: Arc<MarketClient>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Subscribe to prices
    let _ack: CommandAck = client
        .call(
            "subscribe",
            &SubscribePricesRequest {
                symbols: app.subscribed.clone(),
            },
        )
        .await?;

    // Start server streams — prices+state combined, and alerts
    let mut event_stream = client
        .call_server_stream::<_, StreamEvent>("stream_events", &())
        .await?;
    let mut alert_stream = client
        .call_server_stream::<_, AlertResult>("stream_alerts", &())
        .await?;

    let mut render_interval = tokio::time::interval(Duration::from_millis(33));
    let mut event_reader = EventStream::new();

    loop {
        tokio::select! {
            // Terminal key events
            maybe_event = event_reader.next() => {
                if let Some(Ok(Event::Key(key))) = maybe_event {
                    if key.kind == KeyEventKind::Press {
                        let should_fire_rpc = handle_key(app, key.code);

                        if should_fire_rpc {
                            let item = app.current_item;
                            let inputs = app.collect_inputs();
                            let tx = app.rpc_tx.clone();
                            let c = client.clone();
                            let label = item.label().to_string();
                            tokio::spawn(async move {
                                let result = execute_rpc(&c, item, inputs).await;
                                let _ = tx.send((label, result)).await;
                            });
                            app.input_fields.clear();
                        }
                    }
                }
                if app.should_quit { break; }
            }

            // RPC responses (from spawned command tasks)
            maybe_result = app.rpc_rx.recv() => {
                if let Some((label, result)) = maybe_result {
                    app.push_log(label, result);
                    app.cmd_mode = CmdMode::Menu;
                }
            }

            // Server stream: prices + state changes
            maybe_event = event_stream.recv() => {
                match maybe_event {
                    Some(Ok(event)) => app.on_stream_event(event),
                    Some(Err(e)) => {
                        app.conn_status = ConnStatus::Error(e.to_string());
                        break;
                    }
                    None => break,
                }
            }

            // Server stream: alert triggers
            maybe_alert = alert_stream.recv() => {
                if let Some(Ok(alert)) = maybe_alert {
                    app.on_alert(alert);
                }
            }

            // Render tick (~30 fps)
            _ = render_interval.tick() => {
                if app.cmd_mode == CmdMode::Loading {
                    app.spinner = app.spinner.wrapping_add(1);
                }
                terminal.draw(|f| render(f, app))?;
                app.clear_flash();
            }
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(2), client.close()).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let symbols: Vec<String> = if args.symbols.is_empty() {
        vec![]
    } else {
        args.symbols
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    };

    let mut app = App::new(symbols);
    let mut terminal = ratatui::init();

    // Connect before entering TUI (errors print to stderr before alternate screen)
    let (client, resp) = match connect(&args.shm_name, &args.client_name).await {
        Ok(r) => r,
        Err(e) => {
            ratatui::restore();
            eprintln!("Failed to connect: {e}");
            std::process::exit(1);
        }
    };

    app.client_id = resp.client_id;
    app.slot_name = resp.slot_name;
    app.conn_status = ConnStatus::Connected;

    let result = run(&mut terminal, &mut app, Arc::new(client)).await;
    ratatui::restore();
    result
}
