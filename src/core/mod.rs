use tokio::sync::mpsc::Receiver;
use async_trait::async_trait;

#[async_trait]
pub trait CommandHandler<T: Send + 'static> {
    async fn handle(&mut self, cmd: T);
}

pub struct CommandRunner<T, H>
where
    T: Send + 'static,
    H: CommandHandler<T> + Send + 'static,
{
    pub(crate) msg_rx: Receiver<T>,
    pub(crate) handler: H,
}

impl<T, H> CommandRunner<T, H>
where
    T: Send + 'static,
    H: CommandHandler<T> + Send + 'static,
{
    pub fn new(msg_rx: Receiver<T>, handler: H) -> Self {
        CommandRunner { msg_rx, handler }
    }

    pub async fn run(mut self) {
        log::info!("CommandRunner started.");
        while let Some(msg) = self.msg_rx.recv().await {
            self.handler.handle(msg).await;
        }
        log::info!("CommandRunner stopped: channel closed.");
    }
}

