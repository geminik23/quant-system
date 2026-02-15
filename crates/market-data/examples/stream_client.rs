//! Streaming TUI client for the market data service.
//!
//! Connects to the xrpc market data server, subscribes to price ticks and
//! alert notifications, and displays them in a live-updating terminal UI.
//!
//! Usage:
//!   cargo run --example stream_client -- --shm-name market-data --symbols eurusd,xauusd

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
    Block, BorderType, Cell, List, ListItem, Paragraph, Row, Table, TableState,
};
use ratatui::{DefaultTerminal, Frame};

use market_data::rpc_types::*;
use xrpc::{MessageChannelAdapter, RpcClient, SharedMemoryConfig, SharedMemoryFrameTransport};

// ── Types ──

type MarketClient = RpcClient<MessageChannelAdapter<SharedMemoryFrameTransport>>;

#[derive(Parser, Debug)]
#[command(about = "Market data streaming TUI client")]
struct Args {
    #[arg(long, default_value = "market-data")]
    shm_name: String,

    /// Symbols to subscribe (comma-separated, empty = all)
    #[arg(long, default_value = "")]
    symbols: String,

    #[arg(long, default_value = "stream-client")]
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

// ── App State ──

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

enum ConnStatus {
    Connecting,
    Connected,
    Error(String),
}

struct App {
    client_id: usize,
    slot_name: String,
    conn_status: ConnStatus,
    subscribed: Vec<String>,

    prices: Vec<PriceRow>,
    price_idx: HashMap<String, usize>,
    table_state: TableState,

    alerts: VecDeque<AlertRow>,

    tick_count: u64,
    last_update: String,
    should_quit: bool,
}

impl App {
    fn new(symbols: Vec<String>) -> Self {
        Self {
            client_id: 0,
            slot_name: String::new(),
            conn_status: ConnStatus::Connecting,
            subscribed: symbols,
            prices: Vec::new(),
            price_idx: HashMap::new(),
            table_state: TableState::default(),
            alerts: VecDeque::new(),
            tick_count: 0,
            last_update: "--:--:--.---".into(),
            should_quit: false,
        }
    }

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
}

// ── Rendering ──

fn render(f: &mut Frame, app: &mut App) {
    let [header_area, price_area, alert_area, footer_area] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(6),
        Constraint::Length(8),
        Constraint::Length(1),
    ])
    .areas(f.area());

    render_header(f, app, header_area);
    render_price_table(f, app, price_area);
    render_alerts(f, app, alert_area);
    render_footer(f, footer_area);
}

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
        .title(" Market Data Stream ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Cyan));

    let paragraph = Paragraph::new(vec![line1, line2]).block(block);
    f.render_widget(paragraph, area);
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

fn render_footer(f: &mut Frame, area: Rect) {
    let line = Line::from(vec![
        Span::raw("  "),
        Span::styled("[q]", Style::new().fg(Color::Yellow).bold()),
        Span::raw(" Quit  "),
        Span::styled("[Up/Down]", Style::new().fg(Color::Yellow).bold()),
        Span::raw(" Scroll prices"),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

// ── Main Loop ──

async fn run(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    // Connect
    let (client, resp) = connect(&args.shm_name, &args.client_name).await?;
    let client = Arc::new(client);
    app.client_id = resp.client_id;
    app.slot_name = resp.slot_name;
    app.conn_status = ConnStatus::Connected;

    // Subscribe
    let _ack: CommandAck = client
        .call(
            "subscribe",
            &SubscribePricesRequest {
                symbols: app.subscribed.clone(),
            },
        )
        .await?;

    // Start streams
    let mut price_stream = client
        .call_server_stream::<_, PriceTick>("stream_prices", &())
        .await?;
    let mut alert_stream = client
        .call_server_stream::<_, AlertResult>("stream_alerts", &())
        .await?;

    let mut render_interval = tokio::time::interval(Duration::from_millis(33));
    let mut event_reader = EventStream::new();

    loop {
        tokio::select! {
            maybe_event = event_reader.next() => {
                if let Some(Ok(Event::Key(key))) = maybe_event {
                    if key.kind == KeyEventKind::Press {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
                                app.should_quit = true;
                            }
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
                    }
                }
                if app.should_quit { break; }
            }

            maybe_tick = price_stream.recv() => {
                match maybe_tick {
                    Some(Ok(tick)) => app.on_price_tick(tick),
                    Some(Err(e)) => {
                        app.conn_status = ConnStatus::Error(e.to_string());
                        break;
                    }
                    None => break,
                }
            }

            maybe_alert = alert_stream.recv() => {
                if let Some(Ok(alert)) = maybe_alert {
                    app.on_alert(alert);
                }
            }

            _ = render_interval.tick() => {
                terminal.draw(|f| render(f, app))?;
                app.clear_flash();
            }
        }
    }

    let _ = client.close().await;
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
    let result = run(&mut terminal, &mut app, &args).await;
    ratatui::restore();
    result
}
