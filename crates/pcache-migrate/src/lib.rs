pub mod protocol;
#[doc(hidden)]
pub mod test_support;
pub mod transaction;
pub mod writer;

use std::io::Read;
use std::path::Path;

pub fn import_from_reader<R: Read>(
    input: R,
    data_dir: &Path,
    replace: bool,
) -> Result<(), transaction::ImportError> {
    let state = protocol::read(input)?;
    transaction::publish(data_dir, &state, replace).map(|_| ())
}
