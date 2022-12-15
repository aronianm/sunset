class SunRise():

    def __init__(self, df, cx) -> None:
        self.df = df
        self.cx = cx
        self.error = ""

    
    def implicate(self):
        self._assert_columns
        return self.df
    

    def set_db_cols(self, cols):
        self.upsert_cols = cols
    
    @property
    def _assert_columns(self):
        df_cols = self.df.columns.to_list()
        for c in df_cols:
            if c not in self.upsert_cols:
                self.error += f"\n Remove column '{c}' from your dataframe or add to db_columns"
        if len(self.error) > 0:
            raise EnvironmentError(self.error)
