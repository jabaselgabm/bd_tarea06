-- Crea una función en PL/pgSQL llamada obtener_detalle_pedido que 
-- reciba como parámetro el código de un pedido (codigo_pedido) y 
-- devuelva todos los detalles de ese pedido (tabla detalle_pedido). 
-- La función debe devolver un conjunto de resultados 
-- (usar RETURNS TABLE).
drop function if exists obtener_detalle_pedido;

create or replace function obtener_detalle_pedido (p_codigo_pedido int) 
returns table (
	codigo_pedido int4,
	codigo_producto varchar(15),
	cantidad int4,
	precio_unidad decimal(15, 2),
	numero_linea int2
) as $$
begin
	return query select dp.codigo_pedido,  -- Usar el alias dp evita 
						dp.codigo_producto,-- que se confunda con la salida
						dp.cantidad,
						dp.precio_unidad,
						dp.numero_linea from detalle_pedido dp 
		where dp.codigo_pedido = p_codigo_pedido;
end;
$$ language plpgsql;


select * from obtener_detalle_pedido(4);


-- Crea una función llamada calcular_total_pedido que reciba como parámetro 
-- el código de un pedido (codigo_pedido) y calcule el total del pedido 
-- sumando el precio de todos los productos multiplicado por su cantidad. 
-- La función debe devolver el total como un mensaje y como resultado.
drop function if exists calcular_total_pedido;

create or replace function calcular_total_pedido (p_codigo_pedido int4)
returns numeric(15,2) as
$$
declare
	total numeric(15,2) := 0;
	registro record;
begin
	select sum (dp.cantidad * dp.precio_unidad) into total 
		from detalle_pedido dp
		where dp.codigo_pedido = p_codigo_pedido;
	-- Para evitar valores nulos
	total := coalesce (total, 0);
	raise notice 'El pedido % tiene un total de %' , p_codigo_pedido, total;
	return total;
end;
$$ language plpgsql;

select calcular_total_pedido(4);

create or replace function calcular_total_pedido_v2 (p_codigo_pedido int4)
returns numeric(15,2) as
$$
declare
	total numeric(15,2) := 0;
	registro record;
begin
	for registro in 
		select	dp.codigo_producto, 
				dp.cantidad,
				dp.precio_unidad
			from detalle_pedido dp 
			where dp.codigo_pedido = p_codigo_pedido loop
		raise notice '% - % - % -> %', registro.codigo_producto,
									   registro.cantidad,
									   registro.precio_unidad,
									   registro.cantidad * registro.precio_unidad;
		total := total + registro.cantidad * registro.precio_unidad;
	end loop;
	raise notice 'Total pedido: %', total;
	return total;
end;
$$ language plpgsql;

select calcular_total_pedido_v2(2);






-- Crea una función llamada obtener_pedidos_por_estado que reciba como 
-- parámetro el estado de un pedido (estado) y devuelva todos los pedidos 
-- que tengan ese estado. Si el estado es ‘Pendiente’, la función debe 
-- devolver solo los pedidos pendientes. 
-- Si el estado es ‘Entregado’, debe devolver solo los pedidos entregados. 
-- Si el estado es cualquier otro valor, debe devolver un mensaje de error.
drop function if exists obtener_pedidos_por_estado;

create or replace function obtener_pedidos_por_estado (p_estado varchar(15)) 
returns table (
	codigo_pedido int4,
	fecha_pedido date,
	fecha_esperada date,
	fecha_entrega date,
	estado varchar(15),
	comentarios text,
	codigo_cliente int4
) as $$
begin
	if p_estado = 'Entregado' or p_estado = 'Pendiente' then
		return query 
			select 	p.codigo_pedido,
					p.fecha_pedido,
					p.fecha_esperada,
					p.fecha_entrega,
					p.estado,
					p.comentarios,
					p.codigo_cliente			
				from pedido p where p.estado = p_estado;
	else 
		raise exception 'Tipo de pedido no aceptado';
	end if;
end;
$$ language plpgsql;



select * from obtener_pedidos_por_estado ('En curso');


-- Crea una función llamada sumar_stock_productos que reciba como 
-- parámetro un número (incremento) y sume ese valor al stock de 
-- todos los productos de la tabla producto. 
-- La función debe devolver el número total de productos actualizados.
drop function if exists sumar_stock_productos;

create or replace function sumar_stock_productos (incremento int) returns int
as $$
declare
	total int := 0;
	cur_producto cursor for select codigo_producto, cantidad_en_stock 
								from producto;
	v_codigo_producto varchar(15);
	v_cantidad_en_stock int2; -- pequeño ?? pero viene de bd
begin
	-- abrir cursor
	open cur_producto;
	-- recorrer las filas del cursor
	loop
		-- obtener la siguiente fila
		fetch cur_producto into v_codigo_producto, v_cantidad_en_stock;
		-- salir del bucle si no hay más filas
		exit when not found;
		-- realizar el update con el incremento
		update producto
			set cantidad_en_stock = v_cantidad_en_stock + incremento
			where codigo_producto = v_codigo_producto;
		total := total + 1;
	end loop;
	close cur_producto;
	return total;
end;
$$ language plpgsql;

select sumar_stock_productos (10);

select codigo_producto, nombre, cantidad_en_stock  
from producto order by codigo_producto;

drop function if exists sumar_stock_productos_v2;

create or replace function sumar_stock_productos_v2 (incremento int) 
returns int
as $$
declare
	registro record;
	total int := 0;
begin
	-- recorrer las filas 
	for registro in select codigo_producto, cantidad_en_stock from producto loop
		total := total + 1;
		update producto
			set cantidad_en_stock = registro.cantidad_en_stock + incremento
			where codigo_producto = registro.codigo_producto;
		raise notice 'Actualizando registro % ...', total;
	end loop;
	return total;
end;
$$ language plpgsql;

select sumar_stock_productos_v2 (10);

create or replace function sumar_stock_productos_v3 (incremento int) returns int
as $$
declare
	total int := 0;
begin
	with modificados as (
		update producto
			set cantidad_en_stock = cantidad_en_stock + incremento
			returning codigo_producto
	) select count(*) into total from modificados;
	return total;
end;
$$ language plpgsql;

select sumar_stock_productos_v2 (10);


create or replace function sumar_stock_productos_v4 (incremento int) returns int
as $$
declare
	total int := 0;
begin
	update producto
		set cantidad_en_stock = cantidad_en_stock + incremento;
	get diagnostics total = row_count;
	return total;
end;
$$ language plpgsql;

select sumar_stock_productos_v4 (10);