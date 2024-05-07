import socket
from ast import List
from config import DatabaseSettings
from datetime import datetime
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values
from contextlib import contextmanager


db_settings = DatabaseSettings()

PG_USER = db_settings.user
PG_DATABASE = db_settings.database
PG_HOST = db_settings.host
PG_PORT = db_settings.port
PG_SCHEMA = db_settings.schema

DEFAULT_EXECUTION_PAGE_SIZE=1000

# pool defined with 15 live connections
connection_pool = SimpleConnectionPool(1, 15,  user=PG_USER, database=PG_DATABASE,
                                    host=PG_HOST, port=PG_PORT, options=f"-c search_path={PG_SCHEMA}")


@contextmanager
def getcursor():
    con = connection_pool.getconn()
    try:
        yield con.cursor()
    finally:
        con.commit()
        connection_pool.putconn(con)


class SystemEventsRepository:


    def add_record(self, message:str, severity: str, event_type: str, process:str, detail:str, 
                  success: bool=True) -> int:

        host = socket.gethostname()
        event_date = str(datetime.now())

        sql = """
            INSERT INTO system_events
            (message, host, severity, type, date, success, process, detail)
            VALUES
            (%s, %s, %s, %s, %s,%s, %s, %s) RETURNING id           
        """
        prms = (message,host,severity,event_type, event_date, success,process, detail,)

        with getcursor() as cur:
            cur.execute(sql, prms)
            return cur.fetchone()[0]


    def delete_record(self, id:int):

        sql = """
            DELETE FROM system_events
            WHERE ID = %s            
        """

        prms = (id,)

        with getcursor() as cur:
            cur.execute(sql, prms)

    def get_all_records(self):

        result = []

        sql = """
           SELECT  id, host, message, severity, type,
            date, success, process, detail   
           FROM system_events     
        """
        
        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        for row in rows:

            record = {}

            record["id"] = row[0]
            record["host"] = row[1] 
            record["message"] = row[2]
            record["severity"] = row[3] 
            record["event_type"] = row[4]
            record["date"] = row[5] 
            record["success"] = row[6] 
            record["process"] = row[7] 
            record["detail"] = row[8]   

            result.append(record)

        return result


class CrawlStatusRepository:

    def get_last_crawl(self, census_year):

        sql = """
            SELECT max(month_scanned), year_scanned
            FROM
            (
                SELECT month_scanned, max(year_scanned) "year_scanned"
                FROM crawl_status
                WHERE census_year = %s AND date_completed IS NOT NULL
                GROUP BY month_scanned
            ) subquery
            GROUP BY year_scanned
            ORDER BY year_scanned DESC
            LIMIT 1
        """

        prms = (census_year,)

        with getcursor() as cur:
            cur.execute(sql, prms)
            row = cur.fetchone()            

        if row and row[0]:
            result = (row[0], row[1])
        else:
            if census_year == 1960:
                result = (3, 2022)
            elif census_year == 1970:
                result = (3, 2022)
            elif census_year == 1980:
                result = (3, 2022)
            elif census_year == 1990:
                result = (5, 2021)
            else:
                result = (3, 2022)

        return result
        
    
    def add_crawl_operation(self, 
                            census_year: int, 
                            month_scanned: int,
                            year_scanned: int,
                            date_crawled: str, 
                            reels_parsed: int, 
                            images_parsed: int,
                  crawl_time: str) -> int:

        #get the associate crawl status
        crawl_status_id  = self.get_crawl_status(census_year, month_scanned, year_scanned)
     
        if crawl_status_id is None:
            crawl_status_id = self.add_crawl_status(census_year, month_scanned, year_scanned)
        else:
            crawl_status_id = int(crawl_status_id["id"])

        sql = """
                INSERT INTO crawl_operations
                (crawl_status_id, date_crawled, reels_parsed, images_parsed, crawl_time)
                VALUES
                (%s,%s,%s,%s,%s) RETURNING id
        """            

        prms = (crawl_status_id, date_crawled, reels_parsed, images_parsed, crawl_time)

        with getcursor() as cur:
            cur.execute(sql, prms)
            return cur.fetchone()[0]    


    def get_crawl_status(self, census_year: int, month_scanned: int, year_scanned: int):

        sql = """
            SELECT id,census_year,month_scanned, year_scanned,date_completed
            FROM crawl_status
            WHERE census_year = %s AND month_scanned= %s AND year_scanned = %s
        """

        prms = (census_year, month_scanned, year_scanned,)

        with getcursor() as cur:
            cur.execute(sql, prms)
            row = cur.fetchone()    

        if row is not None:
            return {
                "id": row[0],
                "census_year": row[1],
                "month_scanned": row[2],
                "year_scanned": row[3],
                "date_completed": row[4]
            }
        else:
            return None


    def mark_crawl_as_completed(self, census_year: int, month_scanned: int, year_scanned: int) -> None:
        
        sql = """
            UPDATE crawl_status
            SET date_completed = %s
            WHERE census_year = %s AND month_scanned= %s AND year_scanned = %s
        """

        prms = (str(datetime.now()), census_year, month_scanned, year_scanned)

        with getcursor() as cur:
            cur.execute(sql, prms)    

    def delete_crawl_status(self, id: int) -> None:

        crawl_delete_sql = """
            DELETE FROM crawl_status WHERE id = %s
        """
        
        operations_delete_sql = """
            DELETE FROM crawl_operations WHERE 
            crawl_status_id = %s
        """

        prms = (id,)

        with getcursor() as cur:
            cur.execute(operations_delete_sql, prms)
            cur.execute(crawl_delete_sql, prms)

    def add_crawl_status(self, census_year, month_scanned, year_scanned) -> int:

        sql = """
            INSERT INTO crawl_status 
            (census_year, month_scanned, year_scanned)
            VALUES 
            (%s, %s, %s) RETURNING id;
        """

        prms = (census_year, month_scanned, year_scanned,)

        with getcursor() as cur:
            cur.execute(sql, prms)    
            return cur.fetchone()[0]
                
    def get_crawl_status_operations(self, census_year, month_scanned, year_scanned):

        result = []

        sql = """
            SELECT
                crawl_status.id,
                crawl_status.census_year,
                crawl_status.year_scanned,
                crawl_status.month_scanned, 
                crawl_status.date_completed,
                crawl_operations.date_crawled,
                crawl_operations.reels_parsed,
                crawl_operations.images_parsed,
                crawl_operations.crawl_time
            FROM crawl_status INNER JOIN crawl_operations ON
                crawl_status.ID = crawl_operations.crawl_status_id
            WHERE crawl_status.census_year = %s AND month_scanned = %s
                AND year_scanned = %s
        """
        
        prms = (census_year, month_scanned, year_scanned,)

        with getcursor() as cur:
            cur.execute(sql, prms)
            rows = cur.fetchall()

        if rows:
            for row in rows:           
                result.append({
                    "id": row[0],
                    "census_year": row[1],
                    "year_scanned": row[2],
                    "month_scanned": row[3],
                    "date_completed": row[4],
                    "date_crawled": row[5],
                    "reels_parsed": row[6],
                    "images_parsed": row[7],
                    "crawl_time": row[8],
                })

        return result

    def get_all_records(self):

        result = []

        sql = """
            SELECT
                crawl_status.id,
                crawl_status.census_year,
                crawl_status.year_scanned,
                crawl_status.month_scanned, 
                crawl_status.date_completed,
                crawl_operations.date_crawled,
                crawl_operations.reels_parsed,
                crawl_operations.images_parsed,
                crawl_operations.crawl_time
            FROM crawl_status INNER JOIN crawl_operations ON
                crawl_status.ID = crawl_operations.crawl_status_id
           
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:

            for row in rows:
            
                result.append({
                    "id": row[0],
                    "census_year": row[1],
                    "year_scanned": row[2],
                    "month_scanned": row[3],
                    "date_completed": row[4],
                    "date_crawled": row[5],
                    "reels_parsed": row[6],
                    "images_parsed": row[7],
                    "crawl_time": row[8],
                })

        return result

    def reset_crawl(self, scan_month, scan_year, census_year):

        reel_ids = []

        base_sql = """
            SELECT id FROM reels 
            WHERE month_number_scanned = %s AND year_scanned = %s
            AND census_year = %s
        """

        base_params = (scan_month, scan_year, census_year,)

        with getcursor() as cur:
            cur.execute(base_sql, base_params)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                reel_ids.append(int(row[0]))

        if reel_ids:

            with getcursor() as cur:
                #Let's delete the images associated with these reels
                image_delete_sql = """
                    DELETE FROM images 
                    WHERE reel_id IN (%s)
                """ 
                images_delete_params = []

                for reel_id in reel_ids:
                    images_delete_params.append((reel_id,)) 

                execute_values (cur, image_delete_sql, images_delete_params, template=None, page_size=DEFAULT_EXECUTION_PAGE_SIZE)

                #Let's delete the reels
                reel_delete_sql = """
                    DELETE FROM reels
                    WHERE id IN (%s)
                """

                reel_delete_params = []

                for reel_id in reel_ids:
                    reel_delete_params.append((reel_id,)) 

                execute_values (cur, reel_delete_sql, reel_delete_params, template=None, page_size=DEFAULT_EXECUTION_PAGE_SIZE)                   

        crawl_status = self.get_crawl_status(census_year, scan_month, scan_year)

        if crawl_status:
            self.delete_crawl_status(crawl_status["id"])


class ImageRepository:
    
    def record_count(self) -> int:

        sql = """
            SELECT COUNT(id)
            FROM images
        """     

        with getcursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]        

    def add_records(self, reel_id: int, images: List):

        sql = """
            INSERT INTO images
            (
               reel_id, filename, filesize,image_filepath  
            )
            VALUES %s
                  
        """

        sql_params = []

        for image in images:
            sql_params.append(
                (reel_id, str(image["filename"]), int(image["filesize"]), str(image["filepath"]),)
            )

        with getcursor() as cur:
            execute_values (cur, sql, sql_params, template=None, page_size=DEFAULT_EXECUTION_PAGE_SIZE)            


    def add_record(self, reel_id: int, filename: str, filesize: int,
                   image_filepath:str) -> int:

        sql = """
            INSERT INTO images
            (
               reel_id, filename, filesize,image_filepath  
            )
            VALUES
            (%s, %s, %s, %s) RETURNING id            
        """

        prms = (reel_id, filename, filesize, image_filepath,)

        with getcursor() as cur:
            cur.execute(sql, prms)    
            return cur.fetchone()[0]    


    def delete_record(self, id: int):

        sql = """
            DELETE FROM images
            WHERE ID = %s            
        """

        prms = (id,)

        with getcursor() as cur:
            cur.execute(sql, prms)    

    def get_all_records(self):

        result = []

        sql = """
           SELECT id, reel_id, filename, filesize, image_filepath
           FROM images    
        """

        with getcursor() as cur:
            cur.execute(sql)    
            rows = cur.fetchall()

        if rows:

            for row in rows:

                image = {}

                image["id"] = row[0]
                image["reel_id"] = row[1]
                image["filename"] = row[2]
                image["filesize"] = row[3]
                image["image_filepath"] = row[4]

                result.append(image)
        
        return result

class ReelRepository:

    def add_reel_and_images(self, reel) -> None:

        reel_insert_sql = """
                INSERT INTO reels
                (
                    census_year, month_name_scanned, month_number_scanned,
                    year_scanned, scan_identifier  
                )
                VALUES
                (%s, %s, %s, %s, %s) RETURNING id           
        """ 

        image_insert_sql = """
                INSERT INTO images
                (
                    reel_id, filename, filesize,image_filepath  
                )
                VALUES %s
                  
        """

        reel_sql_params = (reel["census_year"], reel["scan_month_name"],
            reel["scan_month"], reel["scan_year"], reel["scan_identifier"])


        with getcursor() as cur:
            cur.execute(reel_insert_sql, reel_sql_params)    
            reel_id = cur.fetchone()[0]            

            #add the associated images (if any)
            if reel["parsed_images"]:
            
                image_sql_params = []        

                for image in reel["parsed_images"]:

                    image_sql_params.append(
                        (reel_id, str(image["filename"]), int(image["filesize"]), str(image["filepath"]),)
                    )

                execute_values (cur, image_insert_sql, image_sql_params, template=None, page_size=DEFAULT_EXECUTION_PAGE_SIZE)           


        #Return the Id
        return reel_id
        
    def record_count(self) -> int:

        sql = """
            SELECT COUNT(id)
            FROM reels
        """     

        with getcursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]        


    def get_reel_by_identifier(self, scan_identifier):

        sql = """
           SELECT * FROM reels 
           WHERE scan_identifier = %s 
        """

        prms = (scan_identifier,)


        with getcursor() as cur:
            cur.execute(sql, prms)
            return cur.fetchall()
              
    def get_filters_by_scan(self, month_number_scanned: int, year_scanned: int, census_year: int):

        result: List[str] = []

        sql = """
           SELECT scan_identifier FROM reels 
           WHERE month_number_scanned = %s and
              year_scanned = %s AND census_year = %s
        """

        prms = (month_number_scanned, year_scanned, census_year)

        with getcursor() as cur:
            cur.execute(sql, prms)
            rows = cur.fetchall()    

        if rows:
            for row in rows:
                result.append(row[0])

        return result

    def get_records_by_scan_date(self, month_number_scanned: int, year_scanned: int, census_year: int):

        result = []

        sql = """
           SELECT * FROM reels 
           WHERE month_number_scanned = %s AND
              year_scanned = %s AND census_year = %s
        """

        prms = (month_number_scanned, year_scanned,census_year,)

        with getcursor() as cur:
            cur.execute(sql, prms)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                reel = {}
                reel["id"] = row[0]
                reel["census_year"] = row[1]
                reel["month_name_scanned"] = row[2]
                reel["month_number_scanned"] = row[3]
                reel["year_scanned"] = row[4]
                reel["scan_identifier"] = row[5]
                reel["snowball_export_date"] = row[6]
                reel["move_flag"] = row[7]
                reel["target_snowball"] = row[8]

                result.append(reel)

        return result

        
    def add_record(self, values:{}):

        sql = """
            INSERT INTO reels
            (
                census_year, month_name_scanned, month_number_scanned,
                year_scanned, scan_identifier  
            )
            VALUES
            (%s, %s, %s, %s, %s) RETURNING id           
        """

        sql_params = (values["census_year"], values["scan_month_name"],
                      values["scan_month"], values["scan_year"], values["scan_identifier"])


        with getcursor() as cur:
            cur.execute(sql, sql_params)
            return cur.fetchone()[0]            

    def delete_record(self, id: int):
        
        sql = """
            DELETE FROM reels
            WHERE ID = %s            
        """
        prms = (id,)
        
        with getcursor() as cur:
            cur.execute(sql, prms)    

    def get_all_records(self):

        result = []

        sql = """
           SELECT id, census_year, month_name_scanned,
            month_number_scanned, year_scanned, scan_identifier,        
            snowball_export_date, move_flag,target_snowball
            FROM reels     
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                reel = {}

                reel["id"] = row[0]
                reel["census_year"] = row[1]
                reel["month_name_scanned"] = row[2]
                reel["month_number_scanned"] = row[3]
                reel["year_scanned"] = row[4]
                reel["scan_identifier"] = row[5]
                reel["snowball_export_date"] = row[6]
                reel["move_flag"] = row[7]
                reel["target_snowball"] = row[8]

                result.append(reel)

        return result

class SnowballRepository:


    def get_unique_snowballs(self):

        result = []

        sql = """
            SELECT DISTINCT reels.target_snowball
            FROM reels
            WHERE reels.target_snowball IS NOT NULL
            ORDER BY target_snowball ASC
        """
        
        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                if row:
                    result.append(row[0])

        return result

    def get_snowballs(self):

        result = []

        sql = """
            SELECT DISTINCT reels.target_snowball, sum(images.filesize) "TOTAL_SNOWBALL_BYTES" 
            FROM reels LEFT outer join images ON
            reels.id = images.reel_id
            WHERE reels.target_snowball IS NOT NULL
            GROUP BY reels.target_snowball;
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                if row[0] is not None and row[1] is not None:
                    record = {"snowball": row[0], "bytes_used": row[1]}
                    result.append(record)
        
        return result

    def get_unassigned_reels(self):

        result = []

        sql = """
            SELECT
            scan_identifier,
            total_image_bytes
            FROM 
            mvw_snowballreadyreels     
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                if row[0] is not None and row[1] is not None:
                    result.append({
                        "reel": row[0],
                        "bytes": row[1]
                    })        
   
        return result

        
    def assign_reels_to_snowball(self, snowball_assignment) -> None:

        """
        {
                "snowball": target_snowball["snowball"], 
                "total_bytes": bytes_alloted,
                "reels": assigned_reels           
            }

        """
        sql = """
            UPDATE reels AS t 
            SET target_snowball = e.snowball
            FROM (VALUES %s) AS e(snowball, identifier) 
            WHERE e.identifier = t.scan_identifier;
        """
        sql_params = []

        target_snowball = snowball_assignment["snowball"]

        for reel in snowball_assignment["reels"]: 
            sql_params.append((target_snowball, reel))

        with getcursor() as cur:
            execute_values (cur, sql, sql_params, template=None, page_size=DEFAULT_EXECUTION_PAGE_SIZE)

    def get_snowball_image_paths(self, snowball):

        result = []

        sql = """
            SELECT DISTINCT image_filepath
            FROM images INNER JOIN reels ON
                images.reel_id = reels.id
            WHERE reels.target_snowball = %s
        """
        sql_params = (snowball, )

        with getcursor() as cur:
            cur.execute(sql, sql_params)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                result.append(row[0])

        return result

    def get_snowball_reels(self, snowball):

        result = []

        sql = """
            SELECT DISTINCT 
                census_year,
                year_scanned,
                month_number_scanned,
                month_name_scanned,
                year_scanned,
                scan_identifier
            FROM reels 
            WHERE reels.target_snowball = %s
        """
        sql_params = (snowball, )

        with getcursor() as cur:
            cur.execute(sql, sql_params)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                result.append(
                    {
                        'census_year': row[0],
                        'scan_year': row[1],
                        'month_number_scanned': row[2],
                        'month_name_scanned': row[3],
                        'year_scanned': row[4],
                        'scan_identifier': row[5]
                    }
                )

        return result

class ReportRepository:

   def get_1960_manifest_data(self):

        result = []

        sql = """
            SELECT * from vw_1960scanmanifest
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                result.append(
                    {
                        "scan_identifier": row[0],
                        "scanned_period": row[1],
                        "reel_location": row[2],
                        "image_count": row[3]
                    }
                )            

        return result
        

   def get_1970_manifest_data(self):
    
        result = []

        sql = """
            SELECT * from vw_1970scanmanifest
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                result.append(
                    {
                        "scan_identifier": row[0],
                        "scanned_period": row[1],
                        "reel_location": row[2],
                        "image_count": row[3]
                    }
                )            

        return result


   def get_1980_manifest_data(self):
        

        result = []

        sql = """
            SELECT * from vw_1980scanmanifest
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                result.append(
                    {
                        "scan_identifier": row[0],
                        "scanned_period": row[1],
                        "reel_location": row[2],
                        "image_count": row[3]
                    }
                )            

        return result


   def get_1990_manifest_data(self):
    
        result = []

        sql = """
            SELECT * from vw_1990scanmanifest
        """

        with getcursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        if rows:
            for row in rows:
                result.append(
                    {
                        "scan_identifier": row[0],
                        "scanned_period": row[1],
                        "reel_location": row[2],
                        "image_count": row[3]
                    }
                )            

        return result


class SnapshotRepository:

    def populate_crawl_image_counts(self):
        
        with getcursor() as cur:

            cur.execute("DROP TABLE IF EXISTS crawl_image_counts")     

            cur.execute("""
                CREATE TABLE crawl_image_counts AS
                    SELECT count(images.id) crawl_count, reels.census_year, reels.year_scanned,
                    reels.month_number_scanned 
                    FROM ips_live.reels INNER JOIN ips_live.images
                    ON reels.id = images.reel_id
                    GROUP BY reels.census_year, reels.year_scanned,
                    reels.month_number_scanned         
            """)




    def get_crawl_counts(self):

        result = []

        with getcursor() as cur:

            cur.execute(
                """
                    SELECT crawl_count, census_year, year_scanned, month_number_scanned
                    FROM crawl_image_counts        
                """
            )

            rows = cur.fetchall()

        if rows:

            for row in rows:
                result.append(
                    {
                        "crawl_count": row[0],
                        "census_year": row[1],
                        "year_scanned": row[2],
                        "month_scanned": row[3]
                    }
                )

        return result

