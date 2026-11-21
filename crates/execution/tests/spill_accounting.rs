//! Live spill bytes (`ARNEB_SPILL_MAX_BYTES` accounting) are released when
//! a spill file goes away, whether it was sealed or abandoned mid-write.
//! A test binary of its own, so no other test spills concurrently and the
//! process-wide counter can be compared exactly.

use std::sync::Arc;

use arneb_execution::spill::{live_spill_bytes, SpillWriter};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

#[test]
fn spill_bytes_are_counted_while_held_and_released_on_drop() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from((0..1024).collect::<Vec<i32>>()))],
    )
    .unwrap();
    assert_eq!(live_spill_bytes(), 0);

    // Sealed: counted through `finish`, released when the file drops.
    let mut writer = SpillWriter::new(Arc::clone(&schema), "accounting_sealed").unwrap();
    writer.write(&batch).unwrap();
    writer.write(&batch).unwrap();
    let held = live_spill_bytes();
    assert!(held > 0);
    assert_eq!(held, writer.bytes_written() as u64);
    let file = writer.finish().unwrap();
    assert_eq!(live_spill_bytes(), held, "sealing keeps the bytes counted");
    let path = file.path().clone();
    drop(file);
    assert_eq!(live_spill_bytes(), 0);
    assert!(!path.exists());

    // Abandoned before `finish` (a failed query): released on drop too.
    let mut writer = SpillWriter::new(schema, "accounting_abandoned").unwrap();
    writer.write(&batch).unwrap();
    assert!(live_spill_bytes() > 0);
    drop(writer);
    assert_eq!(live_spill_bytes(), 0);
}
