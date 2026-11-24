IF DB_ID('RestaurantDB') IS NULL CREATE DATABASE RestaurantDB
GO
USE RestaurantDB
GO
IF OBJECT_ID('SimulationSessions') IS NULL
CREATE TABLE SimulationSessions (
 session_id varchar(36) PRIMARY KEY, status varchar(12) NOT NULL,
 opened_at bigint NOT NULL, sim_time bigint NOT NULL, closed_at bigint NULL,
 expected_events int NULL, generated int NOT NULL DEFAULT 0,
 kitchen nvarchar(max) NOT NULL, pending nvarchar(max) NOT NULL DEFAULT '[]'
)
GO
IF OBJECT_ID('SimulationControl') IS NULL
CREATE TABLE SimulationControl (
 id int PRIMARY KEY CHECK (id=1), session_id varchar(36) REFERENCES SimulationSessions,
 scenario varchar(20) NOT NULL, speed int NOT NULL CHECK(speed IN (1,2,5,10)),
 arrival_factor float NOT NULL CHECK(arrival_factor BETWEEN 0.5 AND 2),
 revision int NOT NULL DEFAULT 0, demo_requested bit NOT NULL DEFAULT 0,
 worker_seen datetime2 NULL
)
IF NOT EXISTS (SELECT 1 FROM SimulationControl WHERE id=1)
 INSERT INTO SimulationControl(id,scenario,speed,arrival_factor) VALUES(1,'Normal Traffic',1,1)
GO
IF OBJECT_ID('ProcessedEvents') IS NULL
CREATE TABLE ProcessedEvents (
 event_id varchar(96) PRIMARY KEY, session_id varchar(36) NOT NULL REFERENCES SimulationSessions,
 event_time bigint NOT NULL, payload nvarchar(max) NOT NULL
)
GO
IF OBJECT_ID('Orders') IS NULL
CREATE TABLE Orders (
 order_id varchar(96) PRIMARY KEY, session_id varchar(36) NOT NULL REFERENCES SimulationSessions,
 placed_at bigint, item_count int, ready_count int, status varchar(12)
)
GO
IF OBJECT_ID('OrderItems') IS NULL
CREATE TABLE OrderItems (
 item_id varchar(96) PRIMARY KEY, order_id varchar(96) NOT NULL REFERENCES Orders,
 session_id varchar(36) NOT NULL REFERENCES SimulationSessions, item_name varchar(80),
 category varchar(60), station_id varchar(24), status varchar(12),
 placed_at bigint, started_at bigint, ready_at bigint, wait_seconds bigint, prep_seconds bigint, sla_seconds int
)
GO
IF OBJECT_ID('StationMetrics') IS NULL
CREATE TABLE StationMetrics (
 session_id varchar(36) REFERENCES SimulationSessions, station_id varchar(24),
 waiting int, preparing int, completed int, avg_prep float, violations int,
 PRIMARY KEY(session_id, station_id)
)
GO
IF OBJECT_ID('CategoryMetrics') IS NULL
CREATE TABLE CategoryMetrics (
 session_id varchar(36) REFERENCES SimulationSessions, category varchar(60),
 completed int, avg_prep float, violations int, PRIMARY KEY(session_id,category)
)
GO
IF OBJECT_ID('SLAAlerts') IS NULL
CREATE TABLE SLAAlerts (
 item_id varchar(96) PRIMARY KEY REFERENCES OrderItems, session_id varchar(36) REFERENCES SimulationSessions,
 status varchar(12), alert_time bigint, delay_seconds bigint
)
GO
IF OBJECT_ID('MetricHistory') IS NULL
CREATE TABLE MetricHistory (
 session_id varchar(36) REFERENCES SimulationSessions, sample_time bigint,
 station_id varchar(24), waiting int, preparing int, PRIMARY KEY(session_id,sample_time,station_id)
)
GO
