from .dev import schedules as dev_schedules
from .preprod import schedules as preprod_schedules
from .newprod import schedules as newprod_schedules

def get_schedule(env, dag_id):
    if env == 'dev':
        return dev_schedules[dag_id]
    elif env == 'preprod':
        return preprod_schedules[dag_id]
    elif env == 'newprod':
        return newprod_schedules[dag_id]
