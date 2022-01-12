def construct_update(column_list):
    final_list = []
    if column_list[0] == '-':
        return ''

    for i in range(len(column_list)):
        astring = column_list[i] + '=' + 'excluded.' + column_list[i]
        final_list.append(astring)
    final_string = ','.join(final_list)
    return final_string

def queryDDL(schema, tablename, layer):
        base = schema.copy()
        pkey = base['column_name'][base['is_primary_key'] == True].iloc[0]
        base["Column"] = base[base.columns].apply(
                            lambda x: ' '.join(x.dropna().astype(str)).replace(" False", "").replace(" True", ""),
                            axis=1)
        columns = base["Column"].tolist()
        query = f"CREATE TABLE IF NOT EXISTS {layer}.{tablename} " + "(" + ", ".join(columns) + f", ingesttime timestamp, ingestby varchar, CONSTRAINT {tablename}_pkey PRIMARY KEY ({pkey}));"
        return query