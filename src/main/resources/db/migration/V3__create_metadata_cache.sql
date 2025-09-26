create table metadata_cache
(
    id           bigserial primary key,
    fqn          varchar(400) not null,
    object_type  varchar(20)  not null,    -- database | schema | table
    service_name varchar(100) not null,
    hash_data    varchar(500) not null,

    constraint metadata_cache_unique unique (fqn)
);
create index metadata_cache_object_type_idx
    on metadata_cache (object_type);