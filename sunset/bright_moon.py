from sqlalchemy import update, insert

class BrightMoon():

    def __init__(self, updates, append, session):
        self.updates = updates
        self.append = append
        self.session = session
    
    def imports(self, table, keys):

        for ux in self.updates:
            idx = { your_key: ux[your_key] for your_key in keys }
            stmt = table.__table__.update(*ux).where(*idx)
                
            
