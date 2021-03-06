import re

def getColumnsFromFile(table_name):
    with open(table_name + '.txt','r') as tableFile:
        cols = tableFile.read().splitlines()
        return set(cols)


if __name__=='__main__':
    with open('query.txt','r') as queryFile:
        query = queryFile.read()
        table1_name = 'adp_po_hdr'
        table2_name = 'adp_po_line'
        table3_name = 'adp_shipto'

        table1_alias = 'ph'
        table2_alias = 'pl'
        table3_alias = 's2'

        table1_columns = getColumnsFromFile(table1_name)
        table2_columns = getColumnsFromFile(table2_name)
        table3_columns = getColumnsFromFile(table3_name)

        t1t2Common = set()
        t2t3Common = set()
        t1t3Common = set()

        t1t2Common = table1_columns.intersection(table2_columns)
        t2t3Common = table2_columns.intersection(table3_columns)
        t1t3Common = table1_columns.intersection(table3_columns)

        t1t2t3Common = table1_columns.intersection(table2_columns).intersection(table3_columns)	

        table1Uniqueertt2 = table1_columns.difference(table2_columns)
        table2Uniqueertt1 = table2_columns.difference(table1_columns)

        table3Uniqueertt2 = table3_columns.difference(table2_columns)
        table2Uniqueertt3 = table2_columns.difference(table3_columns)

        column_names_pattern = r'\{\{column_names\}\}'

        t1t2CommonWithAlias = list(map(lambda x: table1_alias + '.' + x,t1t2Common))
        t1UniqWithAlias = list(map(lambda x: table1_alias + '.' + x,table1Uniqueertt2))
        t2UniqWithAlias = list(map(lambda x: table2_alias + '.' + x,table2Uniqueertt1))
        replaceString = ','.join(list(t1t2CommonWithAlias)) +',' + ','.join(list(t1UniqWithAlias)) +','+ ','.join(list(t2UniqWithAlias))
        query = re.subn(column_names_pattern,replaceString,query,1)[0]

        t2t3CommonWithAlias = list(map(lambda x: table2_alias + '.' + x,t2t3Common))
        t2UniqWithAlias = list(map(lambda x: table2_alias + '.' + x,table2Uniqueertt3))
        t3UniqWithAlias = list(map(lambda x: table3_alias + '.' + x,table3Uniqueertt2))

        replaceString = ','.join(list(t2t3CommonWithAlias)) +',' + ','.join(list(t2UniqWithAlias))  +','+ ','.join(list(t3UniqWithAlias))
        query = re.subn(column_names_pattern,replaceString,query,1)[0]

        cte1Cols = set(list(t1t2Common) + list(table1Uniqueertt2) + list(table2Uniqueertt1))
        cte2Cols = set(list(t2t3Common) + list(table2Uniqueertt3) + list(table3Uniqueertt2))

        c1c2Common = cte1Cols.intersection(cte2Cols)
        c1Unique = cte1Cols.difference(cte2Cols)
        c2Unique = cte2Cols.difference(cte1Cols)

        cte_col_pattern = r'\{\{cte_col\}\}'

        c1c2CommonAlias = list(map(lambda x: 'cte1.'+x,c1c2Common))
        c1Alias = list(map(lambda x: 'cte1.'+x,c1Unique))
        c2Alias = list(map(lambda x: 'cte2.'+x,c2Unique))


        replaceString = ','.join(list(c1c2CommonAlias)) +',' + ','.join(list(c1Alias)) +',' + ','.join(list(c2Alias))
        query = re.subn(cte_col_pattern,replaceString,query,1)[0]

        table1_replace_pattern = r'\{\{table1_name\}\}'
        table2_replace_pattern = r'\{\{table2_name\}\}'
        table3_replace_pattern = r'\{\{table3_name\}\}'

        query = re.subn(table1_replace_pattern,table1_name,query,1)[0]
        query = re.subn(table2_replace_pattern,table2_name,query,1)[0]
        query = re.subn(table2_replace_pattern,table2_name,query,1)[0]
        query = re.subn(table3_replace_pattern,table3_name,query,1)[0]

        with open('finalQuery.sql','w') as finalQuery:
            finalQuery.write(query)


