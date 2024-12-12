from asyncpg import Record

from udata_hydra import config, context


class Resource:
    """Represents a resource in the "catalog" DB table"""

    STATUSES = {
        None: "no status, waiting",
        "BACKOFF": "backoff period for this domain, will be checked later",
        "CRAWLING_URL": "resource URL currently being crawled",
        "TO_ANALYSE_RESOURCE": "resource to be processed for change, type and size analysis",
        "ANALYSING_RESOURCE": "currently being processed for change, type and size analysis",
        "TO_ANALYSE_CSV": "resource content to be analysed by CSV detective",
        "ANALYSING_CSV": "resource content currently being analysed by CSV detective",
        "INSERTING_IN_DB": "currently being inserted in DB",
        "CONVERTING_TO_PARQUET": "currently being converted to Parquet",
    }

    @classmethod
    async def get(cls, resource_id: str, column_name: str = "*") -> Record | None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""SELECT {column_name} FROM catalog WHERE resource_id = '{resource_id}';"""
            return await connection.fetchrow(q)

    @classmethod
    async def insert(
        cls,
        dataset_id: str,
        resource_id: str,
        url: str,
        status: str | None = None,
        priority: bool = True,
    ) -> None:
        if status and status not in cls.STATUSES.keys():
            raise ValueError(f"Invalid status: {status}")

        pool = await context.pool()
        async with pool.acquire() as connection:
            # Insert new resource in catalog table and mark as high priority for crawling
            q = """
                    INSERT INTO catalog (dataset_id, resource_id, url, deleted, status, priority)
                    VALUES ($1, $2, $3, FALSE, $4, $5)
                    ON CONFLICT (resource_id) DO UPDATE SET
                        dataset_id = $1,
                        url = $3,
                        deleted = FALSE,
                        status = $4,
                        priority = $5;"""
            await connection.execute(q, dataset_id, resource_id, url, status, priority)

    @classmethod
    async def update(cls, resource_id: str, data: dict) -> Record:
        """Update a resource in DB with new data and return the updated resource in DB"""
        columns = data.keys()
        # $1, $2...
        placeholders = [f"${x + 1}" for x in range(len(data.values()))]
        set_clause = ",".join([f"{c} = {v}" for c, v in zip(columns, placeholders)])
        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f"""
                    UPDATE catalog
                    SET {set_clause}
                    WHERE resource_id = ${len(placeholders) + 1};"""
            pool = await context.pool()
            return await connection.execute(q, *data.values(), resource_id)

    @classmethod
    async def update_or_insert(
        cls,
        dataset_id: str,
        resource_id: str,
        url: str,
        status: str | None = None,
        priority: bool = True,  # Make resource high priority by default for crawling
    ) -> None:
        if status and status not in cls.STATUSES.keys():
            raise ValueError(f"Invalid status: {status}")

        pool = await context.pool()
        async with pool.acquire() as connection:
            # Check if resource is in catalog then insert or update into table
            if await Resource.get(resource_id):
                q = """
                        UPDATE catalog
                        SET dataset_id = $1, url = $3, status = $4, priority = $5
                        WHERE resource_id = $2;"""
            else:
                q = """
                        INSERT INTO catalog (dataset_id, resource_id, url, deleted, status, priority)
                        VALUES ($1, $2, $3, FALSE, $4, $5)
                        ON CONFLICT (resource_id) DO UPDATE SET
                            dataset_id = $1,
                            url = $3,
                            deleted = FALSE,
                            status = $4,
                            priority = $5;"""
            await connection.execute(q, dataset_id, resource_id, url, status, priority)

    @classmethod
    async def delete(
        cls,
        resource_id: str,
    ) -> None:
        pool = await context.pool()
        async with pool.acquire() as connection:
            # Mark resource as deleted in catalog table
            q = f"""UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource_id}';"""
            await connection.execute(q)

    @staticmethod
    def get_excluded_clause() -> str:
        """Return the WHERE clause to get only resources from the check which:
        - don't have a URL in the excluded URLs patterns
        - are not deleted
        - are not currently being crawled or analysed (i.e. resources with no status, or status 'BACKOFF')
        """
        return " AND ".join(
            [f"catalog.url NOT LIKE '{p}'" for p in config.EXCLUDED_PATTERNS]
            + [
                "catalog.deleted = False",
                "(catalog.status IS NULL OR catalog.status = 'BACKOFF')",
            ]
        )
