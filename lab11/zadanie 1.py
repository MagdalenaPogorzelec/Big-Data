# Databricks notebook source
# MAGIC %md
# MAGIC # Spark UI – Notatka
# MAGIC
# MAGIC **Spark UI** to narzędzie dostępne przez interfejs WWW, które umożliwia monitorowanie i debugowanie aplikacji Spark. Poniżej opisano, co można znaleźć w poszczególnych zakładkach Spark UI:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Jobs
# MAGIC
# MAGIC - Pokazuje listę wszystkich zadań (`jobs`) uruchomionych przez aplikację.
# MAGIC - Można podejrzeć:
# MAGIC   - Liczbę `Stages`
# MAGIC   - Czas wykonania
# MAGIC   - Stan (`Succeeded`, `Failed`)
# MAGIC   - Graficzną reprezentację DAG (Directed Acyclic Graph)
# MAGIC - Kliknięcie na `Job` prowadzi do szczegółowego widoku `Stages`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Stages
# MAGIC
# MAGIC - Każdy `Job` składa się z jednego lub więcej `Stage`.
# MAGIC - Dla każdego etapu widoczne są:
# MAGIC   - Liczba `Tasks`
# MAGIC   - `Shuffle Read/Write`
# MAGIC   - `Input/Output`
# MAGIC   - Czas wykonania
# MAGIC - Przydatne do identyfikacji problemów z partycjonowaniem lub długim czasem zadań.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔹 Storage
# MAGIC
# MAGIC - Pokazuje dane buforowane (`persisted` / `cached`) w RAM.
# MAGIC - Informacje dostępne w tej zakładce:
# MAGIC   - Ilość danych
# MAGIC   - Liczba partycji
# MAGIC   - Lokalizacja (na których węzłach znajdują się dane)
# MAGIC - **Tu znajduje się informacja o dystrybucji danych** – po partycjach i lokalizacji w klastrze.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Executors
# MAGIC
# MAGIC - Lista wszystkich `Executors` oraz `Drivera`.
# MAGIC - Zawiera dane o:
# MAGIC   - Zużyciu pamięci
# MAGIC   - Liczbie zadań (`Tasks`) wykonanych przez executor
# MAGIC   - `Shuffle spill`, dane wejściowe/wyjściowe
# MAGIC - Przydatna do sprawdzania równomiernego rozkładu zadań i zasobów.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  SQL / DataFrame
# MAGIC
# MAGIC - Lista wszystkich zapytań SQL oraz operacji na `DataFrame`.
# MAGIC - Widoczne są:
# MAGIC   - Fazy wykonania (`Physical Plan`)
# MAGIC   - Operacje logiczne i zastosowane optymalizacje
# MAGIC   - Czas trwania poszczególnych etapów
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Gdzie jest informacja o dystrybucji danych?
# MAGIC
# MAGIC - **Storage** – pokazuje dystrybucję partycji danych i ich roz
# MAGIC