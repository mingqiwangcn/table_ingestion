class AggrOP:
    COUNT = 'count'
    COUNT_DISTINCT = 'count(distinct'
    SUM = 'sum'
    MAX = 'max'
    MIN = 'min'

class SQLParser:
    def __init__(self):
        self.aggr_words = {'count(', 'sum(', 'max(', 'min('}
    
    def tokenize(self, sql_text):
        token_lst = []
        sql = sql_text.strip().lower()
        pos = 0
        while pos < len(sql):
            if sql[pos].isspace():
                pos += 1
                continue
            if sql[pos] == "'":
                pos_2 = sql.find("'", pos + 1)
                if pos_2 < 0:
                    return None
                token = sql[pos + 1: pos_2]
                token_info = {'text':token}
                token_lst.append(token_info)
                pos = pos_2 + 1
            else:
                pos_2 = pos
                pos_started = False
                while pos_2 < len(sql) and (not sql[pos_2].isspace()):
                    pos_2 += 1
                    part_text = sql[pos:pos_2]
                    if part_text in self.aggr_words:
                        pos_3 = sql.find(')', pos_2)
                        if pos_3 < 0:
                            return None
                        pos_3 += 1
                        aggr_token = sql[pos:pos_3]
                        token_info = {'text':aggr_token, 'aggr':True}
                        token_lst.append(token_info)
                        pos = pos_3
                        pos_started = True
                        break
                if not pos_started:              
                    token = sql[pos:pos_2].strip()
                    token_info = {'text':token}
                    token_lst.append(token_info)
                    pos = pos_2
        return token_lst

    def parse(self, sql_text):
        token_lst = self.tokenize(sql_text):
        if token_lst[0] != 'select':
            return None
        if token_lst[2] != 'from':
            return None        
        if token_lst[4] != 'where':
            return None

if __name__ == '__main__':
    parser = SQLParser()
    sql_lst = [
        "SELECT level3_description FROM irac_classification WHERE level2 = 'D12';",
        "SELECT COUNT(DISTINCT irac_code) FROM irac_classification WHERE level3 = 'A11A' AND level2_description = 'NERVE ACTION';"
    ]
    token_lst = parser.tokenize(sql_lst[0])
    for token_info in token_lst:
        print(token_info)