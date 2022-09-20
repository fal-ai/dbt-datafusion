from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation


@dataclass(frozen=True, eq=False, repr=False)
class DataFusionRelation(BaseRelation):
    ns_delimit = "."

    def render(self):
        dl = self.ns_delimit
        path = list()
        if self.include_policy.database and self.database:
            path.append(self.database)
        if self.include_policy.schema and self.schema:
            path.append(self.schema)
        if self.name:
            path.append(self.name)

        try:
            ret = dl.join(path)
        except Exception as e:
            raise Exception(str(path))
        return ret
