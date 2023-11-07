
### 1. **Initialize Minikube**:
Start a fresh Minikube cluster:
```bash
minikube start
```

### 2. **Install Helm**:
Ensure Helm, the Kubernetes package manager, is installed. Helm will be used to deploy applications like PostgreSQL and Feast.

### 3. **Install PostgreSQL on Kubernetes**:

**a.** Add the Bitnami Helm repository, which provides a PostgreSQL chart:
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

**b.** Deploy PostgreSQL using Helm:
```bash
helm install my-postgresql bitnami/postgresql
```

### 4. **Retrieve PostgreSQL Password**:
Fetch the auto-generated password for PostgreSQL:
```bash
export POSTGRES_PASSWORD=$(kubectl get secret --namespace default my-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)
```

### 5. **Set Kubernetes Secrets for PostgreSQL Credentials**:
Create a Kubernetes secret to store the PostgreSQL username and password, which will be used by Feast:
```bash
kubectl create secret generic pg-credentials \
--from-literal=PG_USERNAME=postgres \
--from-literal=PG_PASSWORD=$POSTGRES_PASSWORD
```

### 6. **Set Up the PostgreSQL Database**:
At this point, you'd typically set up your database schema, tables, and any initial data. This can be done by connecting to the PostgreSQL instance using tools like `psql` or any PostgreSQL client, or by running SQL scripts.

---

### 7. **Prepare the `feature_store.yaml`**:
Create a `feature_store.yaml` configuration file for Feast with the following content:

```yaml
project: feast_performance
registry: data/registry.db
provider: local

online_store:
  type: postgres
  connection_string: jdbc:postgresql://my-postgresql.default.svc.cluster.local:5432/postgres
  username: ${PG_USERNAME}
  password: ${PG_PASSWORD}

offline_store:
  type: postgres
  connection_string: jdbc:postgresql://my-postgresql.default.svc.cluster.local:5432/postgres
  username: ${PG_USERNAME}
  password: ${PG_PASSWORD}
```

**Explanation of the Connection String:**

- **`jdbc:postgresql:`** This prefix indicates that you're using a JDBC (Java Database Connectivity) connection and that the database type is PostgreSQL.

- **`//my-postgresql.default.svc.cluster.local:5432/`**: This part of the connection string specifies the host and port where your PostgreSQL instance is running.
  
  - **`my-postgresql.default.svc.cluster.local`**: This is the service name for the PostgreSQL instance you deployed on Kubernetes. The format is `<service-name>.<namespace>.svc.cluster.local`. Since you deployed PostgreSQL with the name `my-postgresql` in the `default` namespace, this becomes the hostname. This service name allows other services in the Kubernetes cluster to communicate with PostgreSQL.
  
  - **`:5432`**: This is the default port for PostgreSQL. Unless you've configured a different port during your PostgreSQL deployment, you'd use `5432`.

- **`postgres`**: This is the name of the database you're connecting to. By default, PostgreSQL creates a database with the name `postgres`. You can connect to this database or create and connect to a different one.

The environment variables `${PG_USERNAME}` and `${PG_PASSWORD}` are placeholders that will be replaced by the actual PostgreSQL username and password from the `pg-credentials` Kubernetes secret when Feast components are running in the cluster.

Ensure this file is saved in your working directory.

---

By understanding the connection string, you can ensure that Feast correctly communicates with the PostgreSQL instance running in your Minikube cluster.

Ensure this file is saved in your working directory.

### 8. **Install Feast on Kubernetes**:

**a.** Add the Feast Helm repository and update:
```bash
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update
```

**b.** Deploy Feast using Helm, encoding the `feature_store.yaml` file as a base64 string:
```bash
helm install feast-release feast-charts/feast-feature-server \
    --set feature_store_yaml_base64=$(base64 < feature_store.yaml)
```
**c.** Make updates to Feast using Helm if you have any changes
```bash
helm upgrade feast-release feast-charts/feast-feature-server \
    --set feature_store_yaml_base64=$(base64 < feature_store.yaml)
```
### 9. **Post-Installation Steps**:
After installing Feast, you'd typically define and register features, entities, and other configurations using the Feast CLI or SDK. This involves creating Python scripts to define features, applying them with `feast apply`, and then materializing data into the online store.

---
### 10. **Create a New Project**:
After Feast has been installed you can create a new project by running the 'feast init your-project-name'
This creates a directory with a README.md, an __init__.py and a subdirectory called feature_repo
In the sub-directory you will need to place your feature_store.yaml project config file.  We are using postgre so its going to look something like this:

```bash
project: my_project
provider: local
registry:
    registry_store_type: PostgreSQLRegistryStore
    path: feast_registry
    host: my-postgresql.default.svc.cluster.local
    port: 5432
    database: my_feast_db
    db_schema: public
    user: ${PG_USERNAME}
    password: ${PG_PASSWORD}
online_store:
    type: postgres
    host: my-postgresql.default.svc.cluster.local
    port: 5432
    database: my_feast_db
    db_schema: public
    user: ${PG_USERNAME}
    password: ${PG_PASSWORD}
offline_store:
    type: postgres
    host: my-postgresql.default.svc.cluster.local
    port: 5432
    database: my_feast_db
    db_schema: public
    user: ${PG_USERNAME}
    password: ${PG_PASSWORD}
entity_key_serialization_version: 2
```
