-- insert User data
insert into User(id, username) values (1, 'leo');
insert into User(id, username) values (2, 'jane');
insert into User(id, username) values (3, 'luke');

-- insert Project data
insert into Project(id, name, user_id) values(1, 'zoombies', 1);
insert into Project(id, name, user_id) values(2, 'ninja', 1);
insert into Project(id, name, user_id) values(3, 'vote', 1);

insert into Project(id, name, user_id) values(4, 'reading', 2);
insert into Project(id, name, user_id) values(5, 'writing', 2);

insert into Project(id, name, user_id) values(6, 'game', 3);
