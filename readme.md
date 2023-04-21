# Torun bus schedule parser for Neo4j

 Skrypt przetwarzający rozkład jazdy Torunia i importujący go do grafowej bazy danych Neo4j w formie pozwalającej na wyszukiwanie najszybszych tras pomiędzy przystankami z uwzględnieniem przesiadek.

# Model danych
[Model danych rozkladu jazdy][data_model.png]
* Węzły odpowiadają etapom podróży
* Połączenia odpowiadają przejazdom oraz najwcześniejszym przesiadkom, które zmieniają linię lub kierunek. Z każdym połączeniem wiąże się koszt równy ilości minut potrzebnych na przejazd lub oczekiwanie na przesiadkę.

## Przykład zapytania z wykorzystaniem Graph Data Science

Tworzenie projektu grafu dla GDS
```
CALL gds.graph.project(
    'rozklad',
    'BusStop',
    ['Line', 'LINE_CHANGE'],
    {
        relationshipProperties: 'duration'
    }
)
```

Wyszukanie najszybszej trasy pomiędzy przystankami *Polna* a *Uniwersytet* o godzinie odjazdu między 10:00 a 11:00
```
MATCH (source:BusStop {name: 'Polna'}), (target:BusStop {name: 'Uniwersytet})
WHERE time("10:00") <= time(source.departure) <= time("11:00")
CALL gds.shortestPath.dijkstra.stream('rozklad', {
    sourceNode: source,
    targetNode: target,
    relationshipWeightProperty: 'duration'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, coasts, path
RETURN path order by totalCost limit 1
```
