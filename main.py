from db import DBConnection
from settings import DATABASES

db_credentials = DATABASES.get("game")

DDL_STG_BATTLES = "ddl_stg_battles.sql"
DDL_DIM_BATTLES = "ddl_battles.sql"
DDL_SP = "ddl_sp.sql"
DDL_SP_LOAD = "load.sql"
DDL_SP_TASK2 = "task2.sql"


def create_db_objects():
    with DBConnection(db_credentials) as db:
        query_dr_stg = DBConnection.SqlRaw("DROP TABLE IF EXISTS game.stg_battle")
        db.execute(query_dr_stg, debug=True);
        query_cr_stg = DBConnection.SqlFileTemplate(DDL_STG_BATTLES, {})
        db.execute(query_cr_stg, debug=True);

        query_dr_dim = DBConnection.SqlRaw("DROP TABLE IF EXISTS game.battle")
        db.execute(query_dr_dim, debug=True);
        query_cr_dim = DBConnection.SqlFileTemplate(DDL_DIM_BATTLES, {})
        db.execute(query_cr_dim, debug=True);

        query_dr_sp = DBConnection.SqlRaw("DROP PROCEDURE IF EXISTS game.sp_dim_battle")
        db.execute(query_dr_sp, debug=True);
        query_cr_sp = DBConnection.SqlFileTemplate(DDL_SP, {})
        db.execute(query_cr_sp, debug=True);

        query_dr_sp_load = DBConnection.SqlRaw("DROP PROCEDURE IF EXISTS game.sp_insert_data")
        db.execute(query_dr_sp_load, debug=True);
        query_cr_sp_load = DBConnection.SqlFileTemplate(DDL_SP_LOAD, {})
        db.execute(query_cr_sp_load, debug=True);

        query_cr_sp_task2 = DBConnection.SqlRaw("DROP PROCEDURE IF EXISTS sp_insertrows_data")
        db.execute(query_cr_sp_task2, debug=True);
        query_cr_sp_task2 = DBConnection.SqlFileTemplate(DDL_SP_TASK2, {})
        db.execute(query_cr_sp_task2, debug=True);


def load_data():
    with DBConnection(db_credentials) as db:
        query_load_to_stg = DBConnection.SqlRaw("CALL game.sp_insert_data")
        db.execute(query_load_to_stg, debug=True);

        query_init_load = DBConnection.SqlRaw("CALL game.sp_dim_battle('2023-12-16 02:33:04')")
        db.execute(query_init_load, debug=True);

        query_load = DBConnection.SqlRaw("CALL game.sp_dim_battle('2023-12-16 14:00:43')")
        db.execute(query_load, debug=True);


if __name__ == '__main__':
    create_db_objects()
    load_data()
