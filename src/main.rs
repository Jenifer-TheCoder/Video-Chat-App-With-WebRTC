use std::sync::{Arc, Mutex};
use warp::ws::{Message, WebSocket};
use warp::Filter;
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_stream::wrappers::UnboundedReceiverStream;
use futures_util::stream::StreamExt;

type Clients =  Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Message>>>>;

#[derive(Serialize, Deserialize)]
struct Signal {
    event: String,
    data: String,
    target: Option<String>
}

#[tokio::main]
async fn main() {
    let clients: Clients = Arc::new(Mutex::new(HashMap::new()));


    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(with_clients(clients.clone()))
        .map(move |ws: warp::ws::Ws, clients| {
            ws.on_upgrade(move |socket| handle_connection(socket, clients))
        });
            // Static file route
    let index_route = warp::path("index.html")
    .and(warp::fs::file("./static/index.html"));
    // Combine routes
    let routes = ws_route.or(index_route);
    // Start the server
    warp::serve(routes).run(([127, 0, 0, 1], 8080)).await;

}

fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || clients.clone())
}

async fn handle_connection(ws: WebSocket, clients: Clients) {
    let (user_ws_tx, mut user_ws_rx) = ws.split();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::task::spawn(async move {
        let rx_stream = UnboundedReceiverStream::new(rx);
        let _ = rx_stream.map(Ok).forward(user_ws_tx).await;
    });

    let user_id = uuid::Uuid::new_v4().to_string();
    clients.lock().unwrap().insert(user_id.clone(), tx);

    while let Some(Ok(message)) = user_ws_rx.next().await {
        if let Ok(msg_text) = message.to_str() {
            if let Ok(signal) = serde_json::from_str::<Signal>(msg_text) {
                if let Some(target) = &signal.target {
                    if let Some(target_tx) = clients.lock().unwrap().get(target) {
                        if let Ok(text) = message.to_str() {
                            let _ = target_tx.send(Message::text(text)).unwrap();
                        }
                    }
                }
            }
        }

    }
    clients.lock().unwrap().remove(&user_id);
}