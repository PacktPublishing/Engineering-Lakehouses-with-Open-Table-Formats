-- Ingest data to Bronze layer:
CREATE TABLE nessie.acme_manu
AS (SELECT * FROM "@dipankartnt"."Acme_manufacturing");

-- Create a Virtual Table for Silver Layer:
SELECT
 -- Normalized column naming (snake_case)
 CAST(ProductionVolume AS INTEGER)         AS production_volume,
 CAST(ProductionCost AS FLOAT)             AS production_cost,
 CAST(SupplierQuality AS FLOAT)            AS supplier_quality,
 CAST(DeliveryDelay AS INTEGER)            AS delivery_delay,
 CAST(DefectRate AS FLOAT)                 AS defect_rate,
 CAST(QualityScore AS FLOAT)               AS quality_score,
 CAST(MaintenanceHours AS INTEGER)         AS maintenance_hours,
 CAST(DowntimePercentage AS FLOAT)         AS downtime_pct,
 CAST(InventoryTurnover AS FLOAT)          AS inventory_turnover,
 CAST(StockoutRate AS FLOAT)               AS stockout_rate,
 CAST(WorkerProductivity AS FLOAT)         AS worker_productivity,
 CAST(SafetyIncidents AS INTEGER)          AS safety_incidents,
 CAST(EnergyConsumption AS FLOAT)          AS energy_consumption,
 CAST(EnergyEfficiency AS FLOAT)           AS energy_efficiency,
 CAST(AdditiveProcessTime AS FLOAT)        AS additive_process_time,
 CAST(AdditiveMaterialCost AS FLOAT)       AS additive_material_cost,
 CAST(DefectStatus AS INTEGER)             AS defect_status
FROM nessie."acme_manu"
-- Filter out incomplete records
WHERE ProductionVolume IS NOT NULL
 AND ProductionCost IS NOT NULL
 AND DefectRate IS NOT NULL
 AND QualityScore IS NOT NULL

-- Create the Gold layer dataset:
 WITH numbered_data AS (
 SELECT
   *,
 ROW_NUMBER() OVER (ORDER BY production_volume) - 1 AS row_num
 FROM nessie."silver_acme_manu"
)
SELECT
 DATE_ADD(DATE '2024-01-01', row_num) AS production_date,
 -- Core output & cost metrics
 SUM(production_volume) AS total_production_volume,
 SUM(production_cost) AS total_production_cost,


 CASE
   WHEN SUM(production_volume) > 0 THEN SUM(production_cost) / SUM(production_volume)
   ELSE NULL
 END AS cost_per_unit,
 -- Operational & energy efficiency
 AVG(worker_productivity) AS avg_worker_productivity,
 AVG(energy_efficiency) AS avg_energy_efficiency,
 (0.6 * AVG(worker_productivity) + 0.4 * AVG(energy_efficiency) * 100) AS efficiency_score,
 -- Quality metrics
 AVG(defect_rate) AS avg_defect_rate,
 AVG(quality_score) AS avg_quality_score,
 CASE
   WHEN AVG(defect_rate) >= 5 THEN 'High'
   WHEN AVG(defect_rate) >= 2 THEN 'Moderate'
   ELSE 'Low'
 END AS defect_level,
 -- Workforce and reliability metrics
 SUM(safety_incidents) AS total_safety_incidents,
 AVG(maintenance_hours) AS avg_maintenance_hours,


 -- Inventory and supply chain metrics
 AVG(inventory_turnover) AS avg_inventory_turnover,
 AVG(stockout_rate) AS avg_stockout_rate,
 AVG(supplier_quality) AS avg_supplier_quality,
 AVG(delivery_delay) AS avg_delivery_delay
FROM numbered_data
GROUP BY DATE_ADD(DATE '2024-01-01', row_num)
