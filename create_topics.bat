@echo off
REM ============================================================
REM create_topics.bat
REM Assignment 2 - Streaming Data Analytics
REM Creates the 4 Kafka topics identified in Assignment 1.
REM
REM IMPORTANT: Run this from your Kafka installation folder,
REM   e.g.  C:\kafka\kafka_2.13-3.x.x\
REM or edit KAFKA_HOME below to point to it.
REM Kafka and ZooKeeper (or KRaft) must already be running.
REM ============================================================

set KAFKA_HOME=C:\kafka
set BROKER=localhost:9092

echo Creating topic: pageview-events
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic pageview-events --bootstrap-server %BROKER% --partitions 1 --replication-factor 1

echo Creating topic: cart-events
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic cart-events --bootstrap-server %BROKER% --partitions 1 --replication-factor 1

echo Creating topic: transaction-events
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic transaction-events --bootstrap-server %BROKER% --partitions 1 --replication-factor 1

echo Creating topic: campaign-events
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic campaign-events --bootstrap-server %BROKER% --partitions 1 --replication-factor 1

echo.
echo Done. Listing all topics:
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --list --bootstrap-server %BROKER%

pause
