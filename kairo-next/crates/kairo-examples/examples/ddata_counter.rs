use kairo_examples::ddata_counter::run_ddata_counter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observation = run_ddata_counter("ddata-counter", 5)?;

    println!(
        "{} on {} changed={} observed={} read={}",
        observation.key,
        observation.replica,
        observation.update_changed,
        observation.change_value,
        observation.read_value
    );

    Ok(())
}
