from uuid import uuid5, NAMESPACE_DNS

ROOT_NAMESPACE = uuid5(NAMESPACE_DNS, 'statuses.py')

id = uuid5(ROOT_NAMESPACE, 'sijgtjgoikorfkoerfmkemrf')

print(ROOT_NAMESPACE)
print(id)