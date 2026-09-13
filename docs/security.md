# Security, Session Management & RBAC

This document outlines the security architecture of the database, including the CLI login workflow, user creation, privilege scopes, and custom role assignment.

---

## 1. CLI Authentication Workflow

The CLI connects directly to the database over the custom binary TCP protocol on port `3009` (or the port defined in `db.json`). Sessions are authenticated inside the query shell using a dedicated SQL statement.

### The `LOGIN` Statement
To log in, execute:
```sql
LOGIN <username> '<password>'
```
For example:
```sql
nova> LOGIN admin 'admin'
```

#### Authentication Mechanics:
1. **Validation**: The server computes the PBKDF2/Argon2id hash of the provided password using the unique 32-byte salt stored in the system catalog table `sys.users`.
2. **Session Generation**: If validation succeeds, the server instantiates an active session and returns a single-row grid containing a hexadecimal `session_token`.
3. **Session Tracking**: The CLI automatically extracts this token and attaches it as metadata to every subsequent TCP packet payload. This allows the server to authenticate and authorize requests on the fly.

### Default Superuser Credentials
If security is enabled and `sys.users` is empty, the database automatically provisions a default administrator:
* **Username**: `admin`
* **Password**: `admin`

---

## 2. User & Role Management

The database uses Role-Based Access Control (RBAC) to restrict access to database objects.

### Creating Users
To create a new database user account:
```sql
CREATE USER <username> IDENTIFIED BY '<password>' [ROLE '<role_name>']
```
* If the `ROLE` parameter is omitted, the user defaults to the `read_write` role.
* Example:
  ```sql
  CREATE USER reporting IDENTIFIED BY 'secure_password' ROLE 'read_only';
  ```

### Deleting Users
To delete a user account:
```sql
DROP USER <username>
```
* Example:
  ```sql
  DROP USER reporting;
  ```

---

## 3. Global Roles & Permissions

Every user has an assigned role. The system includes four built-in global roles:

| Role Name | Permissions Description |
| :--- | :--- |
| `admin` | Full access. Bypasses all table-level checks. Can run DDL, DML, user creation, and grant roles. |
| `read_write` | Can perform DML read and write operations (`SELECT`, `INSERT`, `UPDATE`, `DELETE`) on any user table. |
| `read_only` | Can perform only DML read operations (`SELECT`) on any user table. |
| `none` | No default permissions. Access to tables must be granted explicitly via object privileges. |

---

## 4. Fine-Grained Object Privileges

For granular security, users or custom roles can be granted permissions on specific tables.

### Creating Custom Roles
You can define custom, privilege-collecting roles:
```sql
CREATE ROLE <role_name>
```
* Example:
  ```sql
  CREATE ROLE finance_analyst;
  ```

### Granting Privileges
To grant a privilege or assign a role:
```sql
GRANT <privilege> ON <object_name> TO <grantee>
GRANT <role_name> TO <user_name>
```
* **Privilege Types**: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, or `ALL`.
* **Object Name**: The target table name (e.g. `orders`).
* **Grantee**: A user account or another role (supporting role inheritance).
* Examples:
  ```sql
  -- Grant select access on a table
  GRANT SELECT ON orders TO finance_analyst;

  -- Assign a role to a user
  GRANT finance_analyst TO reporting;
  ```

### Revoking Privileges
To remove a privilege or role assignment:
```sql
REVOKE <privilege> ON <object_name> FROM <grantee>
REVOKE <role_name> FROM <user_name>
```
* Examples:
  ```sql
  REVOKE SELECT ON orders FROM finance_analyst;
  REVOKE finance_analyst FROM reporting;
  ```

---

## 5. Security Best Practices
* **Keep `admin` restricted**: Use the default `admin` user only for initial setup, creating custom tables, and provisioning limited user accounts.
* **Use `none` for Sandboxing**: For application databases, assign users the `none` role and explicitly `GRANT` only the required table-level privileges (e.g., `SELECT` on `products`, `INSERT` on `orders`).
