# Masterarbeit Code

Implementierung der Masterarbeit von Stefanie Linke

# Beschreibung
Das Programm erzeugt Binary Strings und sucht die Strings, welche die Hamming-Distanz 1 haben, mithilfe von fünf verschiedenen Algorithmen.
Außerdem werden die Laufzeiten und verschiedene Eigenschaften der Algorithmen gemessen.

# Ausführung
Das Programm bitstrings.py aus dem Ordner Berechnung kann auf Standardhardware als normales Python Programm ausgeführt werden. Die Parameter b und k, sowie der name der Ausgabefile müssen im Code eingestellt werden.

```
python3 bitstrings.py 
```

Es wird eine CSV-Datei erstellt. Diese Datei muss in HDSF auf dem ARA Cluster hochgeldaen werden.

```
hdsf dfs -put bitstrings_b8_k1.csv
```
Danach können die Algorithmen zur Berechnung der Hamming-Distanz aus dem Ordner Berechnung ebenfalls auf dem ARA CLuster ausgeführt werden.

```
spark-submit serial.py
```

Will man die Algorithmen auf die Laufzeiten und verschiedene Eigenschaften untersuchen, so muss man die Programme aus dem Ordner Messungen auf die gleiche Weise ausführen.

```
spark-submit serial_data.py
```
