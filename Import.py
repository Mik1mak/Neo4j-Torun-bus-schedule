import os
import json
import re
import itertools
import time
from datetime import datetime, timedelta
from neo4j import GraphDatabase

data_folder = "data"
uri = "neo4j://localhost:7687"
password = "viking-spain-wave-biscuit-atlanta-9292"

def striptime(r, delta: timedelta):
    min_tab = []
    for m in r[1]:
        minute = re.sub(r'[^0-9]', '', m)
        if minute != '':
            min_tab.append(minute)
    return [(datetime.strptime(f"{r[0]}:{min}", "%H:%M") + delta).time() for min in min_tab]

def stripminutes(minutes: str):
    if re.search(r'\d', minutes):
        return int(minutes)
    else:
        return 0

def parse(parse_method, data):
    line=data['nazwa_linii']
    dir=data['opis_linii'][10:]
    last_node = None
    for trasa_node in data['trasa (czas, nazwa)']:
        if trasa_node != ['min | przystanek']:
            departures = list(itertools.chain.from_iterable(map(
                    lambda r: striptime(r, timedelta(minutes=stripminutes(trasa_node[0]))),
                    data['odjazdy (hh:mm)'])))
            parse_method(trasa_node, last_node, line, dir, departures)
            last_node = trasa_node

def create_bus_stops(tx, data):
    def create_bus_stop(node, last_node, line, dir, departures):
        for dep in departures:
            tx.run("CREATE (a:BusStop {name: $name, line: $line, direction: $dir, departure: $dep})", 
                name=node[1], 
                line=line, 
                dir=dir,
                dep=dep)
    parse(create_bus_stop, data)
    print("Created BusStops in:", (time.time()-start), "s since start")

def create_line_changes(tx, data):
    def create_line_change(node, last_node, line, dir, departures):
        name=node[1]
        for dep in departures:
            other_lines_in_bus_stop = tx.run("""
                MATCH
                    (a:BusStop{name: $name, line: $line, direction: $dir, departure: $dep}),
                    (b:BusStop{name: $name})
                WHERE a.departure < b.departure and (a.direction <> b.direction or a.line <> b.line)
                RETURN DISTINCT b.line as line, b.direction as direction
                """,
                    name=name, 
                    line=line,
                    dep=dep,
                    dir=dir)
            for line_dir in other_lines_in_bus_stop:
                other_direction=line_dir['direction']
                other_line=line_dir['line']
                tx.run("""
                    MATCH 
                        (a:BusStop{name: $name, line: $line, direction: $dir, departure: $dep}),
                        (b:BusStop{name: $name, line: $other_line, direction: $other_direction})
                    WHERE a.departure < b.departure and (a.direction <> b.direction or a.line <> b.line)
                    WITH a, b ORDER BY duration.between(a.departure, b.departure) LIMIT 1
                    CREATE (a)-[c:LINE_CHANGE{duration: duration.between(a.departure, b.departure).minutes}]->(b)
                    """, 
                        name=name, 
                        line=line, 
                        dir=dir,
                        dep=dep,
                        other_line=other_line,
                        other_direction=other_direction)
    parse(create_line_change, data)
    print("Created LINE_CHANGEs in: ", (time.time()-start), "s since start")

def create_lines(tx, data):
    def create_line(node, last_node, line, dir, departures):
        if last_node != None:
            length = stripminutes(node[0]) - stripminutes(last_node[0])
            tx.run("""
                MATCH 
                    (a:BusStop {name: $from_node, line: $line, direction: $direction}), 
                    (b:BusStop {name: $to_node, line: $line, direction: $direction})
                WHERE duration.between(a.departure, b.departure).minutes = $len
                CREATE (a)-[r:LINE {duration: $len}]->(b)
                RETURN type(r)""", 
                    from_node=last_node[1],     
                    to_node=node[1],
                    line=line,
                    direction=dir,
                    len=length)
    parse(create_line, data)
    print("Created LINEs in: ", (time.time()-start), "s since start")

start = time.time()

dir_path = os.path.join(os.getcwd(), data_folder)
data_files = []

for path in os.listdir(dir_path):
    if os.path.isfile(os.path.join(dir_path, path)):
        data_files.append(os.path.join(data_folder, path))

driver = GraphDatabase.driver(uri, auth=("neo4j", password))
data_objs = []

for fileName in data_files:
    file = open(fileName, mode="r", encoding="utf-8")
    data_objs.append(json.load(file)) 
    file.close()

with driver.session() as session:
    for method in [create_bus_stops, create_line_changes, create_lines]:
            for data in data_objs:
                    session.execute_write(method, data)

driver.close()

print("Imported in: ", (time.time()-start), "s")