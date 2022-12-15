import pandas as pd

class SunBurn():

    def create_tx(self):
        cols = ", ".join(self.df.columns.to_list())
        db_df = pd.read_sql(f'select {cols} from {self.table.__tablename__}', self.cx)
        upsertable_df = self.df.merge(db_df, on=self.uniq_keys, indicator=True, how='left')
        update_arrays = []
        append_arrays = []
        for i, row in upsertable_df.iterrows():
            if row['_merge'] == 'both':
                del row["_merge"]
                update_arrays.append(row.to_dict())
            elif row['_merge'] == 'left_only':
                del row["_merge"]
                append_arrays.append(row.to_dict())
        return update_arrays, append_arrays
