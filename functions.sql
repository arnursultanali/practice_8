-- Function 1: Get the rows by pattern

create or replace function get_by_pattern(pattern_type text, pattern text)
returns table(first_name varchar(255), last_name varchar(255), phone_number varchar(15), extra_info varchar(255))
language plpgsql as $$
    begin
        if pattern_type = 'first_name' then
            return query
            select contact_first_name, contact_last_name, contact_number, contact_extra_info
            from contacts where contact_first_name ilike '%' || pattern || '%';
        elsif pattern_type = 'last_name' then
            return query
            select contact_first_name, contact_last_name, contact_number, contact_extra_info
            from contacts where contact_last_name ilike '%' || pattern || '%';
        elsif pattern_type = 'number' then
            return query
            select contact_first_name, contact_last_name, contact_number, contact_extra_info
            from contacts where contact_number ilike '%' || pattern || '%';
        else
        raise notice '[Error]: Invalid attribute_type!';
        end if;
    end;
$$;


-- Function 2: Paginate the rows
create or replace function query_pagination(rows_per_page int, page_number int)
returns table(first_name varchar(255), last_name varchar(255), phone_number varchar(15), email varchar(511), extra_info varchar(255))
language plpgsql as $$
begin
    return query
    select contact_first_name, contact_last_name, contact_number, contact_email, contact_extra_info from contacts limit rows_per_page offset (page_number - 1) * rows_per_page;
end;
$$;