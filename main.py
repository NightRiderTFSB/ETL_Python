from sqlalchemy import create_engine, MetaData, Table, select, func, text

# Configurar la conexión a la base de datos
engine = create_engine('mysql://starboy:starboyc00l@localhost/sakila')
metadata = MetaData()
metadata.bind = engine
metadata.reflect(bind=engine)

# Obtener las tablas reflejadas
rental_table = metadata.tables['rental']
inventory_table = metadata.tables['inventory']
film_table = metadata.tables['film']

# Construir la consulta para obtener las películas más alquiladas
query = (
    select(film_table.c.title, film_table.c.film_id, func.count(rental_table.c.inventory_id).label('rental_count'))
    .select_from(
        rental_table.join(inventory_table, rental_table.c.inventory_id == inventory_table.c.inventory_id)
        .join(film_table, inventory_table.c.film_id == film_table.c.film_id)
    )
    .group_by(film_table.c.film_id)
    .order_by(func.count(rental_table.c.inventory_id).desc())
    .limit(10)
)
'''
# Ejecutar la consulta y obtener los resultados
with engine.connect() as connection:
    result = connection.execute(query)
    for row in result:
        print(row)'''

# Crear una vista en la base de datos a partir de la consulta
view_name = "top_rented_films"
create_view_query = text(f"CREATE OR REPLACE VIEW {view_name} AS {str(query.compile(engine, compile_kwargs={'literal_binds': True}))}")

with engine.begin() as conn:
    conn.execute(create_view_query)
    print(f"Vista '{view_name}' creada exitosamente.")

# Consultar la información de la vista
view = Table(view_name, metadata, autoload_with=engine)

# Consultar la información de la vista
with engine.connect() as connection:
    result = connection.execute(select(view))
    for row in result:
        print(row)