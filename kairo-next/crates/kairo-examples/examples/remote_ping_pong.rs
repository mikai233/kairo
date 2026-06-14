use kairo_examples::remote_ping_pong::run_remote_ping_pong;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_remote_ping_pong("remote-ping-pong", 41)?;

    println!(
        "remote ping {} reached {}, pong {} returned to {}",
        observation.ping_value,
        observation.responder_path,
        observation.pong_value,
        observation.reply_path
    );

    Ok(())
}
