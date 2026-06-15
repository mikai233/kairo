use super::*;

struct UnitActor;

impl Actor for UnitActor {
    type Msg = ();

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

#[test]
fn actor_system_testkit_dead_letter_probe_receives_stopped_actor_send() {
    let kit = ActorSystemTestKit::new("testkit-dead-letter-probe").expect("system should build");
    let probe = kit
        .create_dead_letter_probe("dead-letters")
        .expect("dead-letter probe should spawn");
    let subject = kit
        .system()
        .spawn("subject", Props::new(|| UnitActor))
        .expect("subject should spawn");

    kit.system().stop(&subject);
    assert!(subject.wait_for_stop(Duration::from_secs(1)));
    subject.tell(()).expect_err("send after stop should fail");

    let dead_letter = probe
        .expect_msg(Duration::from_millis(50))
        .expect("dead-letter probe should observe stopped send");
    assert_eq!(dead_letter.recipient(), subject.path());
    assert_eq!(dead_letter.message_type(), std::any::type_name::<()>());
    assert_eq!(dead_letter.reason(), "actor is stopped");
    kit.shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}

#[test]
fn actor_harness_dead_letter_probe_receives_subject_dead_letters() {
    let harness = ActorHarness::spawn(
        "harness-dead-letter-probe",
        "subject",
        Props::new(|| UnitActor),
    )
    .expect("harness should spawn subject");
    let probe = harness
        .create_dead_letter_probe("dead-letters")
        .expect("dead-letter probe should spawn");
    let subject = harness.actor_ref();

    harness.stop();
    harness
        .expect_stopped(Duration::from_secs(1))
        .expect("subject should stop");
    subject.tell(()).expect_err("send after stop should fail");

    let dead_letter = probe
        .expect_msg(Duration::from_millis(50))
        .expect("dead-letter probe should observe stopped send");
    assert_eq!(dead_letter.recipient(), subject.path());
    assert_eq!(dead_letter.reason(), "actor is stopped");
    harness
        .shutdown(Duration::from_secs(1))
        .expect("system should terminate");
}
