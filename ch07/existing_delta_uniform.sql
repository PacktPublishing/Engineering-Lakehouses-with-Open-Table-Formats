ALTER TABLE existing_delta_table
SET TBLPROPERTIES (
 'delta.minReaderVersion' = '2',
 'delta.minWriterVersion' = '5',

  'delta.enableIcebergCompatV2' = 'true', 
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
