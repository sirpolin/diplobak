create table response
(
    response_id      integer not null
        constraint response_pk
            primary key,
    last_update      timestamp,
    bank_name        varchar(150) default NULL::character varying,
    city             varchar(150) default NULL::character varying,
    title            text,
    fulltext         text,
    datetime         timestamp,
    mark             integer      default 0,
    num_views        integer      default 0,
    num_comments     integer      default 0,
    status           varchar(50)  default NULL::character varying,
    debitcards       boolean      default false,
    creditcards      boolean      default false,
    hypothec         boolean      default false,
    autocredits      boolean      default false,
    credits          boolean      default false,
    restructing      boolean      default false,
    deposits         boolean      default false,
    investments      boolean      default false,
    transfers        boolean      default false,
    remote           boolean      default false,
    corporate        boolean      default false,
    rko              boolean      default false,
    acquiring        boolean      default false,
    salary_project   boolean      default false,
    businessdeposits boolean      default false,
    businesscredits  boolean      default false,
    bank_guarantee   boolean      default false,
    leasing          boolean      default false,
    business_other   boolean      default false,
    business_remote  boolean      default false
);

alter table response
    owner to oleg;

create unique index response_response_id_uindex
    on response (response_id);

