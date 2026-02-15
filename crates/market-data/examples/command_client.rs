//! Command REPL TUI client for the market data service.
//!
//! Connects to the xrpc market data server and provides an interactive menu
//! for all unary RPC commands: ping, get state, get symbols, get price,
//! set alert, remove alert, etc.
//!
//! Usage:
//!   cargo run --example command_client -- --shm-name market-data

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind};
use futures::StreamExt;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use market_data::rpc_types::*;
use xrpc::{MessageChannelAdapter, RpcClient, SharedMemoryFrameTransport};

// ── Types ──

type MarketClient = RpcClient<MessageChannelAdapter<SharedMemoryFrameTransport>>;

#[derive(Parser, Debug)]
#[command(about = "Market data command REPL TUI client")]
struct Args {
    #[arg(long, default_value = "market-data")]
    shm_name: String,

    #[arg(long, default_value = "command-client")]
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

    let transport = SharedMemoryFrameTransport::connect_client(&resp.slot_name)?;
    let channel = MessageChannelAdapter::new(transport);
    let client = RpcClient::new(channel);
    let _handle = client.start();

    Ok((client, resp))
}

// ── Menu ──

#[derive(Clone, Copy, PartialEq)]
enum MenuItem {
    Ping,
    GetState,
    GetSymbols,
    GetPrice,
    GetPrices,
    SetAlert,
    RemoveAlert,
    Quit,
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
        Self::Quit,
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
            Self::Quit => "Quit",
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

// ── UI Mode ──

#[derive(Clone, PartialEq)]
enum UiMode {
    Menu,
    Input { field_index: usize },
    Loading,
}

// ── Log Entry ──

struct LogEntry {
    label: String,
    result: String,
}

// ── App State ──

struct App {
    client_id: usize,
    slot_name: String,
    connected: bool,

    selected_menu: usize,
    menu_state: ListState,
    mode: UiMode,

    input_fields: Vec<(String, String)>, // (label, value)
    current_item: MenuItem,

    log: VecDeque<LogEntry>,
    log_scroll: u16,

    spinner: u8,
    should_quit: bool,

    rpc_tx: tokio::sync::mpsc::Sender<(String, String)>,
    rpc_rx: tokio::sync::mpsc::Receiver<(String, String)>,
}

impl App {
    fn new() -> Self {
        let (rpc_tx, rpc_rx) = tokio::sync::mpsc::channel(4);
        Self {
            client_id: 0,
            slot_name: String::new(),
            connected: false,
            selected_menu: 0,
            menu_state: ListState::default().with_selected(Some(0)),
            mode: UiMode::Menu,
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
        if item == MenuItem::Quit {
            self.should_quit = true;
            return;
        }
        self.current_item = item;
        let fields = item.input_fields();
        if fields.is_empty() {
            self.mode = UiMode::Loading;
        } else {
            self.input_fields = fields
                .iter()
                .map(|&label| (label.to_string(), String::new()))
                .collect();
            self.mode = UiMode::Input { field_index: 0 };
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
        self.log_scroll = 0; // auto-scroll to bottom on new entry
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
        MenuItem::Quit => unreachable!(),
    }
}

// ── Rendering ──

fn render(f: &mut Frame, app: &mut App) {
    let [header_area, body_area, footer_area] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(10),
        Constraint::Length(1),
    ])
    .areas(f.area());

    render_header(f, app, header_area);

    let [menu_area, right_area] =
        Layout::horizontal([Constraint::Length(22), Constraint::Min(30)]).areas(body_area);

    render_menu(f, app, menu_area);
    render_right_panel(f, app, right_area);
    render_footer(f, app, footer_area);
}

fn render_header(f: &mut Frame, app: &App, area: Rect) {
    let status_style = if app.connected {
        Style::new().fg(Color::Green).bold()
    } else {
        Style::new().fg(Color::Red).bold()
    };
    let status_text = if app.connected {
        "CONNECTED"
    } else {
        "DISCONNECTED"
    };

    let line = Line::from(vec![
        Span::styled(" Status: ", Style::new().bold()),
        Span::styled(status_text, status_style),
        Span::raw("    "),
        Span::styled("Client ID: ", Style::new().bold()),
        Span::raw(app.client_id.to_string()),
        Span::raw("    "),
        Span::styled("Slot: ", Style::new().bold()),
        Span::styled(&app.slot_name, Style::new().fg(Color::DarkGray)),
    ]);

    let block = Block::bordered()
        .title(" Market Data Commander ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::Magenta));

    let paragraph = Paragraph::new(line).block(block);
    f.render_widget(paragraph, area);
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
            } else if *item == MenuItem::Quit {
                Style::new().fg(Color::Red)
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
    match &app.mode {
        UiMode::Input { field_index } => {
            let field_count = app.input_fields.len();
            let input_height = (field_count as u16) + 5; // borders + title + hint + padding
            let [input_area, log_area] = Layout::vertical([
                Constraint::Length(input_height.min(area.height / 2)),
                Constraint::Min(4),
            ])
            .areas(area);

            render_input_form(f, app, *field_index, input_area);
            render_log(f, app, log_area);
        }
        UiMode::Loading => {
            let [loading_area, log_area] =
                Layout::vertical([Constraint::Length(3), Constraint::Min(4)]).areas(area);

            render_loading(f, app.spinner, loading_area);
            render_log(f, app, log_area);
        }
        UiMode::Menu => {
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

fn render_footer(f: &mut Frame, app: &App, area: Rect) {
    let hints = match &app.mode {
        UiMode::Menu => vec![
            Span::raw("  "),
            Span::styled("[Up/Down]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Navigate  "),
            Span::styled("[Enter]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Select  "),
            Span::styled("[PgUp/PgDn]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Scroll log  "),
            Span::styled("[q]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Quit"),
        ],
        UiMode::Input { .. } => vec![
            Span::raw("  "),
            Span::styled("[Enter]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Next field  "),
            Span::styled("[Esc]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Cancel  "),
            Span::styled("[Tab]", Style::new().fg(Color::Yellow).bold()),
            Span::raw(" Submit all"),
        ],
        UiMode::Loading => vec![
            Span::raw("  "),
            Span::styled("Executing RPC...", Style::new().fg(Color::Yellow)),
        ],
    };
    f.render_widget(Paragraph::new(Line::from(hints)), area);
}

// ── Event Handling ──

fn handle_key_menu(app: &mut App, code: KeyCode) {
    match code {
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
}

fn handle_key_input(app: &mut App, code: KeyCode, field_index: usize) {
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
                app.mode = UiMode::Input {
                    field_index: field_index + 1,
                };
            } else {
                // All fields filled — submit
                app.mode = UiMode::Loading;
            }
        }
        KeyCode::Tab => {
            // Submit immediately regardless of which field we're on
            app.mode = UiMode::Loading;
        }
        KeyCode::Esc => {
            app.input_fields.clear();
            app.mode = UiMode::Menu;
        }
        _ => {}
    }
}

// ── Main Loop ──

async fn run(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    client: Arc<MarketClient>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut render_interval = tokio::time::interval(Duration::from_millis(33));
    let mut event_reader = EventStream::new();

    loop {
        tokio::select! {
            maybe_event = event_reader.next() => {
                if let Some(Ok(Event::Key(key))) = maybe_event {
                    if key.kind == KeyEventKind::Press {
                        match &app.mode {
                            UiMode::Menu => handle_key_menu(app, key.code),
                            UiMode::Input { field_index } => {
                                let fi = *field_index;
                                handle_key_input(app, key.code, fi);
                            }
                            UiMode::Loading => {
                                // Ignore input while loading
                            }
                        }
                    }
                }
                if app.should_quit { break; }

                // Check if we need to fire an RPC (mode just changed to Loading)
                if app.mode == UiMode::Loading {
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

            maybe_result = app.rpc_rx.recv() => {
                if let Some((label, result)) = maybe_result {
                    app.push_log(label, result);
                    app.mode = UiMode::Menu;
                }
            }

            _ = render_interval.tick() => {
                if app.mode == UiMode::Loading {
                    app.spinner = app.spinner.wrapping_add(1);
                }
                terminal.draw(|f| render(f, app))?;
            }
        }
    }

    let _ = client.close().await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut app = App::new();
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
    app.connected = true;

    let result = run(&mut terminal, &mut app, Arc::new(client)).await;
    ratatui::restore();
    result
}
