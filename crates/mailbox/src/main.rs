use mailbox_harness::protocol_version;
use mailbox_protocol::PROTOCOL_VERSION;

fn main() {
    // Keep the binary linked to both workspace crates from day one so the
    // scaffold fails loudly if the workspace graph breaks.
    assert_eq!(protocol_version(), PROTOCOL_VERSION);
    println!("agent-mailbox {PROTOCOL_VERSION}");
}
