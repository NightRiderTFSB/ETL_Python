from sqlalchemy import create_engine, MetaData, Table

# Configurar la conexión a la base de datos
engine = create_engine('mysql://starboy:starboyc00l@localhost/sakila')
metadata = MetaData(bind=engine)