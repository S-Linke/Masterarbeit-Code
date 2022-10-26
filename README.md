# Masterarbeit Code

Implementierung der Masterarbeit von Stefanie Linke

# Beschreibung
Das Programm erzeugt Binary Strings und sucht die Strings, welche die Hamming-Distanz 1 haben, mithilfe von fünf verschiedenen Algorithmen.
Außerdem werden die Laufzeiten und verschiedene Eigenschaften der Algorithmen gemessen.

# Ausführung
Das Programm bitstrings.py kann auf STandardhardware ausgeführt werden. Die Parameter b und k, sowie der name der Ausgabefile müssen im Code eingestellt werden.

```
python3 bitstrings.py 
```

Es wird eine CSV-Datei erstellt. Diese Datei muss in HDSF auf dem ARA Cluster hochgeldaen werden.

```
hdsf dfs -put bitstrings_b8_k1.csv
```
Danach können die Algorithmen zur Berechnung der Hamming-Distanz ebenfalls auf dem ARA CLuster ausgeführt werden.

```
spark-submit serial.py
```
