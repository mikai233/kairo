use super::*;

#[derive(Clone)]
enum EventStreamProbeMsg {
    Event(&'static str),
    Subscribe(mpsc::Sender<bool>),
    Unsubscribe(mpsc::Sender<bool>),
}

#[derive(Clone)]
struct OtherEvent;

struct EventStreamProbe {
    observed: mpsc::Sender<&'static str>,
}

impl Actor for EventStreamProbe {
    type Msg = EventStreamProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            EventStreamProbeMsg::Event(label) => {
                self.observed
                    .send(label)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            EventStreamProbeMsg::Subscribe(reply_to) => {
                let subscribed = ctx.event_stream().subscribe(ctx.myself());
                reply_to
                    .send(subscribed)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            EventStreamProbeMsg::Unsubscribe(reply_to) => {
                let unsubscribed = ctx.event_stream().unsubscribe(&ctx.myself());
                reply_to
                    .send(unsubscribed)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn event_stream_publishes_to_typed_subscribers_once() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(first_tx))
        .unwrap();
    actor
        .tell(EventStreamProbeMsg::Subscribe(second_tx))
        .unwrap();

    assert!(first_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    assert!(!second_rx.recv_timeout(Duration::from_secs(1)).unwrap());

    system
        .event_stream()
        .publish(EventStreamProbeMsg::Event("event"));

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "event"
    );
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn event_stream_unsubscribe_stops_delivery() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (subscribe_tx, subscribe_rx) = mpsc::channel();
    let (unsubscribe_tx, unsubscribe_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(subscribe_tx))
        .unwrap();
    subscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor
        .tell(EventStreamProbeMsg::Unsubscribe(unsubscribe_tx))
        .unwrap();

    assert!(unsubscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    system
        .event_stream()
        .publish(EventStreamProbeMsg::Event("event"));

    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn event_stream_matches_exact_event_type() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (subscribe_tx, subscribe_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(subscribe_tx))
        .unwrap();
    subscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.event_stream().publish(OtherEvent);
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}
