-- Procedure 1: Single insertion of an user
create or replace procedure insert_user(first_name varchar(255), last_name varchar(255), cnumber varchar(15), email varchar(511), extra_info varchar(255))
language plpgsql as $$
declare
    existing_id int;
begin
    /* check if the user exists */
    select contact_id into existing_id
    from contacts
    where contact_first_name=first_name and contact_last_name=last_name;

    if found then
        update contacts set contact_number=cnumber where contact_id=existing_id;
    else
        insert into contacts(contact_first_name, contact_last_name, contact_number, contact_email, contact_extra_info) values(
            first_name, last_name, cnumber, email, extra_info
        );
    end if;
end;
$$;

/* Procedure 2: multiple insertion through an array*/

create type user_type as (
    first_name varchar(255),
    last_name varchar(255),
    phone_number varchar(15),
    email varchar(511),
    extra_info varchar(255)
);

create or replace procedure multiple_insertion(users user_type[])
language plpgsql as $$
declare
    u user_type;
    invalid_users user_type[] := '{}';
begin
    -- looping through array users
    foreach u in array users loop
        if u.phone_number !~ '[0-9]{5,15}$' then
            invalid_users := array_append(invalid_users, u);
            continue;
        end if;

        if exists (select 1 from contacts where u.first_name = contact_first_name and u.last_name = contact_last_name) then
            update contacts
            set contact_number = u.phone_number, contact_email = u.email, contact_extra_info = u.extra_info
            where contact_first_name = u.first_name and contact_last_name = u.last_name;
        else
            insert into contacts(contact_first_name, contact_last_name, contact_number, contact_email, contact_extra_info) values(
                u.first_name, u.last_name, u.phone_number, u.email, u.extra_info
            );
        end if;
    end loop;

    if array_length(invalid_users, 1) > 0 then
        raise notice '[Warning]: Invalid users: %', invalid_users;
        end if;
end;
$$;


-- Procedure 3: Delete user by user_name or phone_number
create or replace procedure delete_user(first_name varchar(255), last_name varchar(255), phone_number varchar(15))
language plpgsql as $$
begin
    -- check if the user exists
    if exists (select 1 from contacts where (contact_first_name=first_name and contact_last_name=last_name) or (contact_number=phone_number)) then
        delete from contacts where (contact_first_name=first_name and contact_last_name=last_name) or (contact_number=phone_number);
    else
        raise notice '[Error] The contact with first_name %, last_name % and phone_number % not found', first_name, last_name, phone_number;
    end if;

end;
$$;