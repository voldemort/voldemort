insert into test_table (key_, value_) values (?, ?) on duplicate key update value_ = ?
create table test_table (key_ varchar(200) not null primary key, value_ varchar(200)) engine=InnoDB;