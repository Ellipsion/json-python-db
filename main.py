import json

DB_PATH = "./{}.json"
CURRENT_DB = None
class Pysql:
    def __init__(self) -> None:
        self.db_path = "./{}.json"
        self.current_db = None
        self.running = True

    def exec_command(self, command):
        command = command[:-1].lower()
        micros = command.split(" ")

        if (micros[0] == "quit"):
            self.running = False

        if (micros[0] == "use") and (len(micros)==2):
            self.use_db(micros[1])

        if (micros[0] == "create") and len(micros)>=3:
            if (micros[1] == "table") and len(micros)>3:
                table_name = micros[2]
                if table_name[-1] == "(":
                    table_name = table_name[:-1]
                tuple_ = " ".join(micros[3:])[1:-1]
                attributes = tuple_.split(",")

                self.create_table(table_name, attributes)
        
            if (micros[1] == "database"):
                db_name = micros[2]
                self.create_db(db_name)
        
        if (micros[0] == "show") and len(micros) == 2:
            if (micros[1] == "tables"):
                self.show_tables()
            if micros[1] == "databases":
                self.show_databases()

        if (micros[0] == "desc") and len(micros) == 2:
            self.desc_table(micros[1])

        if (micros[0] == "insert") and (micros[1] == "into") and (micros[3][:6] == "values"):
            table_name = micros[2]
            tuple_ = " ".join(micros[4:])[:-1]
            if tuple_[0] == "(":
                tuple_ = tuple_[1:]
            
            values = tuple_.split(",")
            self.insert_into_table(table_name, values)
            
        print(micros)
        print("Executed", "< {} >".format(command))

    def exists_in_db_list(self, db_name):
        with open("databases"+".json", "r") as f:
            dbs = json.loads(f.read())
        return (db_name in dbs)

    def update_db_list(self, db_name):
        with open("databases"+".json", "r") as f:
            dbs = json.loads(f.read())
        if db_name not in dbs:
            dbs.append(db_name)
            with open("databases"+".json", "w") as f:
                f.write(json.dumps(dbs))
            return True
        else:
            print("Database "+db_name.upper()+" already exists...")
            return False

    def create_db(self, db_name):
        exists = self.update_db_list(db_name)
        db = {
            "database_name": db_name,
            "tables": [],
        }
        if exists:
            with open(db_name+".json", "w") as f:
                f.write(json.dumps(db))
        else:
            print("Database "+db_name.upper()+" already exists...")

    def use_db(self, db_name):
        print(db_name)
        if self.exists_in_db_list(db_name):
            with open(DB_PATH.format(db_name), "r") as f:
                db = json.loads(f.read())
            self.current_db = db
        else:
            print("database "+db_name.upper()+" does not exists...")
    
    def close_db(self):
        if self.current_db is not None:
            with open(self.db_path.format(self.current_db["database_name"]), "w") as f:
                f.write(json.dumps(self.current_db))

    def show_databases(self):
        with open("databases.json", "r") as f:
            dbs = json.loads(f.read())
        print("databases")
        for i in dbs:
            print("| "+i+" |")


    def create_table(self, table_name, attributes):
        if table_name not in self.current_db["tables"]:
            
            table = {}
            table["table_name"] = table_name
            table["attributes"] = {}
            # table["tuples"] = []

            for attribute in attributes:
                properties = attribute.split(" ")
                if '' in properties:
                    properties.remove('')
                # print(attr_props)
                attribute_obj = {}
                attribute_obj["field"] = properties[0]
                attribute_obj["type"] = properties[1]
                attribute_obj["constraints"] = [c for c in properties[2:]]
                attribute_obj["values"] = []
                table["attributes"][properties[0]] = attribute_obj

            self.current_db["tables"] = [table_name]
            self.current_db[table_name] = table
            self.close_db()
        else:
            print("Table "+table_name.uppper()+" already exists...")
        # self.update_json("database", db)


    def show_tables(self):
        if self.current_db != None:
            print("Tables in "+ self.current_db["database_name"])
            for table in self.current_db["tables"]:
                print("| ", table, " |")
        else:
            print("No database selected")

    def insert_into_table(self, table_name, values):
        if self.current_db != None:
            if table_name in self.current_db["tables"]:
                values = [self.clean_data(x) for x in values]
                # data = []
                # index = 0
                # for field in self.current_db[table_name]["attributes"]:
                #     if self.current_db[table_name]["attributes"][field]["type"].startswith("int"):
                #         self.current_db[table_name]["attributes"][field]["values"].append(int(values[index]))
                #         # data.append(int(values[index]))
                #     else:
                #         self.current_db[table_name]["attributes"][field]["values"].append(int(values[index]))
                #         # data.append(values[index])
                #     index += 1
            else:
                print("table does not exit")
        else:
            print("select a db")
            
    def desc_table(self, table_name):
        if self.current_db != None:
            if table_name in self.current_db["tables"]:
                attributes = self.current_db[table_name]["attributes"]
                print("field", " -> ", "type")
                for key in attributes:
                    print(attributes[key]["field"], " -> ", attributes[key]["type"])
            else:
                print("table does not exit")
        else:
            print("select a db")

    def run(self):      
        while self.running:
            command = input("pysql> ")
            while True:
                if command[-1] != ";":
                    command += " " + input("     > ")
                else:
                    if command.lower() == "quit;":
                        break
                    self.exec_command(command)
                    break
            if command.lower() == "quit;":
                break

    def clean_space(self, data):
        if type(data[0]) == str:
            data = data[1:]
        return data

pysql = Pysql()
pysql.run()