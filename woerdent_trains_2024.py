import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import matplotlib as mt
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    services = mo.sql(
        f"""
        SELECT * FROM 'https://blobs.duckdb.org/nl-railway/services-2024.csv.gz'
        """
    )
    return (services,)


@app.cell
def _(mo, services):
    woerden_services = mo.sql(
        f"""
        SELECT * FROM services WHERE "Stop:Station code" = 'WD';
        """
    )
    return (woerden_services,)


@app.cell
def _(mo, services, woerden_services):
    next_stations = mo.sql(
        f"""
        SELECT services."Service:RDT-ID", min(services."Stop:Arrival time") as next_station_arr, argmin(services."Stop:Station name", services."Stop:Arrival time") as next_station from services JOIN woerden_services ON services."Service:RDT-ID" = woerden_services."Service:RDT-ID" and services."Stop:Arrival time" > woerden_services."Stop:Departure time" GROUP BY ALL ORDER BY next_station_arr;
        """
    )
    return (next_stations,)


@app.cell
def _(mo, services, woerden_services):
    prev_stations = mo.sql(
        f"""
        SELECT services."Service:RDT-ID", max(services."Stop:Departure time") as prev_station_dep, ARGMAX(services."Stop:Station name", services."Stop:Departure time") as prev_station from services JOIN woerden_services ON services."Service:RDT-ID" = woerden_services."Service:RDT-ID" and services."Stop:Departure time" < woerden_services."Stop:Arrival time" GROUP BY ALL ORDER BY prev_station_dep;
        """
    )
    return (prev_stations,)


@app.cell
def _(mo, next_stations):
    next_counts = mo.sql(
        f"""
        SELECT next_station, count(*) as cnt FROM next_stations GROUP BY ALL ORDER BY cnt desc
        """
    )
    return (next_counts,)


@app.cell
def _(mo, prev_stations):
    prev_counts = mo.sql(
        f"""
        SELECT prev_station, count(*) as cnt FROM prev_stations GROUP BY ALL ORDER BY cnt desc
        """
    )
    return (prev_counts,)


@app.cell
def _(mo, prev_counts):
    _df = mo.sql(
        f"""
        SELECT SUM(cnt) FROM prev_counts
        """
    )
    return


@app.cell
def _(mo, prev_counts):
    selected_station = mo.ui.dropdown(options=prev_counts["prev_station"])
    selected_station
    return (selected_station,)


@app.cell
def _(mo, prev_stations, selected_station, services):
    _df = mo.sql(
        f"""
        SELECT * FROM services JOIN (SELECT "Service:RDT-ID" as ride_id, prev_station FROM prev_stations WHERE prev_station = '{selected_station.value}') as prev_rides ON services."Service:RDT-ID" = prev_rides.ride_id AND services."Stop:Station name" = prev_rides.prev_station ORDER BY "Stop:Departure Time"
        """
    )
    return


@app.cell
def _(mo, next_counts):
    next_selected_station = mo.ui.dropdown(options=next_counts["next_station"])
    next_selected_station
    return (next_selected_station,)


@app.cell
def _(mo, next_selected_station, next_stations, services):
    _df = mo.sql(
        f"""
        SELECT * FROM services JOIN (SELECT "Service:RDT-ID" as ride_id, next_station FROM next_stations WHERE next_station = '{next_selected_station.value}') as next_rides ON services."Service:RDT-ID" = next_rides.ride_id AND services."Stop:Station name" = next_rides.next_station ORDER BY "Stop:Departure Time"
        """
    )
    return


@app.cell
def _(mo, next_stations, prev_stations):
    adj_stations = mo.sql(
        f"""
        SELECT * FROM prev_stations full join next_stations ON prev_stations."Service:RDT-ID" = next_stations."Service:RDT-ID"
        """
    )
    return (adj_stations,)


@app.cell
def _(adj_stations, mo):
    adj_stations_counts = mo.sql(
        f"""
        SELECT prev_station, next_station, count(*) as cnt from adj_stations GROUP BY ALL order by cnt desc
        """
    )
    return (adj_stations_counts,)


@app.cell
def _(adj_stations_counts):
    all_stations = list(set(adj_stations_counts["prev_station"]) | set(adj_stations_counts["next_station"]))
    all_stations
    return (all_stations,)


@app.cell
def _():
    colors = [
        "#EA638C",
        "#B33C86",
        "#190E4F",
        "#03012C",
        "#002A22",
        "#993955",
        "#AE76A6",
        "#A3C3D9",
        "#CCD6EB",
        "#BAD1CD",
        "#F2D1C9",
        "#E086D3",
        "#8332AC",
        "#8332AC",
        "#BE6E46"
    ]
    colors
    return (colors,)


@app.cell
def _(all_stations):
    labels = [st if st is not None else "<Terminal>" for st in all_stations*2]
    source_indexes = {st: i for i, st in enumerate(all_stations)}
    destination_indexes = {st: i + len(source_indexes) for i, st in enumerate(all_stations)}
    return destination_indexes, labels, source_indexes


@app.cell
def _(adj_stations_counts, destination_indexes, source_indexes):
    source = []
    target = []
    value = []
    for row in adj_stations_counts.iter_rows(named=True):
        sidx = source_indexes[row["prev_station"]]
        tidx = destination_indexes[row["next_station"]]
        val = row["cnt"]
        source.append(sidx)
        target.append(tidx)
        value.append(val)
    return source, target, value


@app.cell
def _(colors, labels, source, target, value):
    import plotly.graph_objects as go

    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 15,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = labels,
          color = colors*2,
        ),
        link = dict(
          source = source, # indices correspond to labels, eg A1, A2, A1, B1, ...
          target = target,
          value = value
      ))])

    fig.update_layout(title_text="Stations", font_size=10)
    fig.show()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
