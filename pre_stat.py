

class Stat():
    def __init__(self, name):
        self.name = name
        self.stats = {}
    
    def increment(self, stats_ref, system_id):
        if stats_ref not in self.stats:
            self.stats[stats_ref] = {}
            self.stats[stats_ref]["general"] = 0

        self.stats[stats_ref]["general"] += 1
        if system_id not in self.stats[stats_ref]:
            self.stats[stats_ref][system_id] = 1
        else:
            self.stats[stats_ref][system_id] += 1

    def add(self, stats_ref, system_id, amount):
        if stats_ref not in self.stats:
            self.stats[stats_ref] = {}
            self.stats[stats_ref]["general"] = amount

        self.stats[stats_ref]["general"] += amount
        if system_id not in self.stats[stats_ref]:
            self.stats[stats_ref][system_id] = amount
        else:
            self.stats[stats_ref][system_id] += amount

    def set_value(self, system_id, stats_ref):
        pass

    def get_value(self):
        print(self.name)
        print(self.stats)
        return

    def store(self, conn, date):
        cur = conn.cursor()
        for stat_ref, data in self.stats.items():
            for system_id, value in data.items():
                self.store_record(cur, date, stat_ref, system_id, value)
        conn.commit()
        self.reset()

    def store_record(self, cur, date, stat_ref, system_id, value):
        if system_id == "general":
            system_id = None
        stmt = """INSERT INTO
             stats_pre_process (date, zone_ref, stat_description, system_id, value)
             VALUES (%s, %s, %s, %s, %s)"""
        cur.execute(stmt, (date, stat_ref, self.name, system_id, value))

    def reset(self):
        self.stats = {}