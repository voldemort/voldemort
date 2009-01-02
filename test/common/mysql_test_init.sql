use test;
create table test_store (
  key_ varbinary(100) not null,
  version_ blob not null,
  value_ blob,
  primary key (key_, version_(100))
) engine = InnoDB;
