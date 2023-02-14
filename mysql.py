import pymysql
import utils

db = pymysql.connect(user='handelsregister_un', passwd='handelsregister_pw', host='localhost',
                     port=3307, db='handelsregister_db', use_unicode=True, charset='utf8')
cursor = db.cursor()

cursor.execute(utils.CREATE_COMPANY_TABLE)

cursor.execute(utils.CREATE_CEO_TABLE)

cursor.execute(utils.CREATE_HISTORY_TABLE)

cursor.execute(utils.CREATE_PROCURA_TABLE)

cursor.execute(utils.CREATE_LOCATION_TABLE)

cursor.execute(utils.CREATE_CONTACT_TABLE)

db.commit()
