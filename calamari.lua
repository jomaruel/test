function  fatal(mensaje, ...)
    io.stderr:write(string.format(mensaje, table.unpack{...}))
    io.stderr:write("\n*** Pulse <Intro> para continuar\n")
    io.stderr:flush()
    io.stdin:read()
    os.exit(false)  
end

local separadores = 
{ ["\t"]= "[ ]*\t[ ]*", [","]= "%s*,%s*", [";"]= "%s*;%s*",["|"]= "|", ["="]= "%s*=%s*" }

-- ===================== *append_slash* ===========================
-- append a slash to dir if it hasn't one. slash may be "/" or "\\"
local function append_slash(dir, slash)    
     if dir and not dir:find(slash, #dir, true)  then
        return dir..slash
    end        
    return dir
end

-- ===================== *exists* ================================
-- check if file or directory exists. The trick os.rename(file, file) 
-- is taken from the WEB and works in Linux and Windows even de 13 
-- error code that indicates that the file exists but permission is denied
-- returns Ok and error message 
local function exists (file, is_a_dir, slash)
    if file then 
        if is_a_dir then file = append_slash(file, slash) end		
        local ok, err, code = os.rename(file, file)
		if code == 13 then ok = true end
        return ok, err
    end  
    return false, ""
end

function trim(s)
	return s:match( "^%s*(.-)%s*$" )
end
-- ===================== *get_os_separators* ====================
function get_os_separators()  
    local c, islinux, iswindows
    
    if package.config then
        c = package.config:sub(1,1)
    end    
    if c == nil then
        if iswindows then 
            c = "\\"
        else
            c = "/"
        end
    end    
    SLASH = c
    NL = (c == "/" and '\n' or "\r\n"    )
end 
-- ===================== *get_os_separators* ====================
function get_os_separators(iswindows)  
    local c    
    if package.config then
        c = package.config:sub(1,1)
    end    
    if c == nil then
        if iswindows then 
            c = "\\"
        else
            c = "/"
        end
    end    
    SLASH = c
    NL = (c == "/" and '\n' or "\r\n"    )
end 

function get_locale(iswindows)  
    local c    
    if package.config then
        c = package.config:sub(1,1)
    end    
    if c == nil then
        if iswindows then 
            c = "\\"
        else
            c = "/"
        end
    end    
    SLASH = c
    NL = c == "/" and '\n' or "\r\n"    
end 

function  trocea(str, sep)       
    local n=  1
    local sep= separadores[sep]
    local pos,c = 1
    local tab = {}
    
    while true do
        local i,j = string.find(str, sep, pos)
        if  i  and i <= #str  then
            tab[n] = string.sub(str, pos, i-1)      
            pos = j +1
            n = n + 1            
        else    
            tab[n] = string.sub(str, pos, limit)  
            break
        end    
    end       
    if str:byte(1) == 32 then        
        c = 1 repeat c = c + 1 until str:byte(c)   ~= 32
        tab[1] = string.sub(tab[1], c)   
    end
    if str:byte(#str) == 32 then
        local s = tab[n]
        c = #s        
        repeat c = c - 1 until  s:byte(c) ~= 32
        tab[n] = string.sub(s, 1, c)   
    end
    return tab
end 
function centra(cadena, ancho, car_relleno)
    if cadena then
        car_relleno = car_relleno or " "
        ancho = ancho - #cadena
        
        if ancho >0 then
            local temp, izquierda
            izquierda =  ancho // 2
            temp = string.rep(car_relleno, izquierda)
            if izquierda == ancho - izquierda then 
                car_relleno = "";
            end
            return string.format("%s%s%s%s", temp, cadena, temp, car_relleno)        
        end
    end    
    return cadena
end

function sum(vector)
    local s = 0
    for _,v in pairs(vector) do
        s = s + v
    end
    return s
end

function sumi(vector)
    local s = 0
    for j = 1, #vector do
        s = s + v[i]
    end
    return s
end



local DEBUG = 1

function  show_var(var, margen)   
    margen = margen or 0
    if type(var) == "table" then
        io.stderr:write("(tabla)\n")        
        for k, v  in pairs(var) do
            io.stderr:write(
                        string.format("%s[%-s]:", 
                        string.rep(" ",margen), tostring(k))
                        )        
            show_var(v, #tostring(k)+3)    
        end        
    else
        io.stderr:write(tostring(var)," ", type(var) , "\n")
    end  
end 

function  disp(nombre_var, var)

        if not var then
            show_var(nombre_var)    
        else        
            io.stderr:write(string.format("%q = ", nombre_var))
            show_var(var)
        end
    
end

function  dispe(nombre_var, var)
    if DEBUG then        
        disp(nombre_var, var)
        os.exit(false)
    end
end

function sep_miles(i, sep)    
    --i = math.floor(i) -- esto  ya lo hace el llamante
    if i < 0 then 
        return "-"..sep_miles(-i, sep)
    end        
    if i < 1000 then return tostring(i) end
    local v = i//1000    
    if v > 999 then
        return ("%s%s%03d"):format(sep_miles(v, sep), sep, (i - v *1000))            
    end    
    return ("%s%s%03d"):format(v, sep, (i - v *1000))            
end


-----------
 
local SIMPLIFICARSE_LA_VIDA = 1

local powers10 = {1,10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,10000000000}

function fmt(f, decs, sep)
    
    if f~=f or f == nil then
        return "N/A"
    end
    if f >= math.huge  or f <= -math.huge then
        return "Inf"
    end
    
    local i, d = math.modf(f)
    
    decs = decs or 0
    if d == 0 then
        if decs > 0 then
            return ("%s%s%s"):format(sep_miles(i,"."), "," , string.rep("0", decs))
        else
            return sep_miles(i, ".")
        end    
    end    
    
    if decs == 0 then
        if i>=0 then
            
            return sep_miles(d >=0.5 and i +1 or i, ".")
        end
        return sep_miles(d <=-0.5 and i -1 or i, ".")
    end           
    return string.format(string.format("%%.%df", decs), f)
    
    
    
end



    pad = 
        function (str, w, left)   
            if w <= #str then
                return str
            end    
            if left then
                return string.format("%s%s",string.rep("*", w-#str), str)
            end
            return string.format("%s%s", str, string.rep("*", w-#str))
        end
    
    put_num = 
        function (num, w, precision, left)   
            return pad(fmt(num, precision), w, left) 
        end
    
    put_str = 
        function (s, w, precision, left)   
        local str
        if precision and precision > 0 then
            str = string.sub(tostring(s), 1, precision)    
        else
            str = tostring(s)
        end        
        return pad(str, w, left) 
    end
    
    put_num_plus = 
        function (num, w, precision, left)  
            if num and num > 0 then
                return pad("+"..fmt(num, precision), w, left) 
            end    
            return pad(fmt(num, precision), w, left) 
        end
    


Table = {    
    fmt_funcs = {
        i = function(val, fmt)                
                return put_num(val, fmt.width, fmt.precision, true)  end,
        f = function(val, fmt)
                return put_num(val, fmt.width, fmt.precision, true)  end,
        s = function(val, fmt)
                return put_str(val, fmt.width, fmt.precision, true)  end,
        S = function(val, fmt)
                return put_str(val, fmt.width, fmt.precision, false) end,
    }
}    

function Table.put_header(self)
    local s = table.concat(self.col_names,  self.separator).."\n"
    io.stdout:write(s)
end

function Table.put_line(self, line_values)
    if line_values then
        local m = math.min(#line_values, #self.col_names)
        local t = {}
        for j = 1, m do
            local f = self.col_fmt[j]
            t[j] =  Table.fmt_funcs[f.letter](line_values[j], f)
        end
        local f = io.stdout
        f:write(table.concat(t, self.separator).."\n")
        
    end
end
function Table.close(self)
    print("Close")
end
function Table.new(header_cols, separator)
    
    local tab = {
            col_names = {},
            col_fmt = {}            
        }
    setmetatable(tab, {__index = Table})    
                
    for j = 1, #header_cols do       

        local w, precision, letter = header_cols[j][2]:match("(%d*)%.?(%d*)([sSiIfFpP])")        
        w = w == "" and 0 or w+0
        precision = precision == "" and 0 or precision+0        
        local col_name   = header_cols[j][1]        
        if letter == 'i'  then            
            w = w + (w-1)//3            
        elseif letter == 'f' then 
            w = w + precision + 1
        end    
        if w > #col_name then
            col_name = centra(col_name, w)
        else
           w = #col_name
        end                                    
        tab.col_names[j] = col_name
        tab.col_fmt[j]   =  {letter = letter, width = w, precision = precision}
    end
    tab.separator = separator
    return tab    
end

function basename(path) 
    return string.match(path, "[^/\\:]+$")
end

function set_locale(str, iswindows) 
    if str == "EN" then
        os.setlocale(iswindows and 'English_United States' or 'en_US', "all")
    elseif str == "" then  
        os.setlocale('')
    else
        os.setlocale(iswindows and 'Spanish_Spain' or 'es_ES', "all")
    end     
end

--assert(os.setlocale('C'))
col = 1
s =("\"123,aa'\r"):match("^['\"][^'\"]*['\"\r\n]")
n = "12.3490E+01 +1"
n = "12.88E+1 +1"
if n:find("E", 1, true) then
    s = n:match("^%d+%.?%d*E[%-+]?%d+")
else
    s = n:match("^%d+%.?%d+")
end   
if s then 
    print(s)
    print (tonumber(s))
else
    s = n:match(".+[%s]+")
    print("NO", s)
end    
disp("FIN")
print(s,"*", s:byte(1))
if s:byte(#s) ~= 39 then 
    print ("NO CERRADO") 
else
    print(s)
    col = col + #s
end
disp("")

assert(os.setlocale('Spanish_Spain', "all"))
print (string.find("á", "a", 1, true))
print(1.3)
disp(os.setlocale())
--assert(os.setlocale('Spanish_Spain', 'numeric'))
--assert(os.setlocale('Spanish_Spain'))
--assert(os.setlocale('ES_es'))
--assert(os.setlocale('English_United States'))
--assert(os.setlocale('C'))

ta ={ 
{"Provincia", "8.4s"}, {"Pob.", "6s"}, {"Pob.(%)", ".2f"}        
    }

tb = {
        {"1", "2.2f"}, 
        --{"2", "2.7f"},
        
    }


t = Table.new(tb, "|")

t:put_line{1.299}
t:put_header()


t = Table.new(ta, "|")
t:put_header()
t:put_line{"ALBACETE", 2, 1}
t:put_line{"MADRID", 2, 1}
t:put_line{"SEVILLA", 2, 1}





--print(fmt(-1/0, 0))
--print(fmt(-1.149, 2))
--print(fmt(1202.499999999999, 0))


function cod_elec(anio, mes, dia)
cod_pais = 034
tip_elec = 102

--return dia | mes *2^6 | anio*2^10 | tip_elec*2^22 | cod_pais*2^26

end
function decod_elec(i)

--return i&M_ANIO, i&M_MES, i&M_DIA

end

i = cod_elec(2024, 6, 17)
print(decod_elec(i))
os.exit(0)

local map = 
{
{{}, "1800-01-01",function(val) Field_updateDateRdcTl_Result =  val end}
}

function vectores_para_motor(mapa_vectores, longitud_max)

	for _, v  in ipairs(mapa_vectores)  do
		local vector = v[1]
		for j =1, longitud_max do
			if vector[j] == nil or  vector[j]=="" then vector[j] = v[2] end		
		end
		-- llamo a la función que asigna el valor (uso concat para ello)
		v[3](table.concat(vector or {}, "#", 1, longitud_max).."#")
	end
end	
vectores_para_motor(map,3)
print(Field_updateDateRdcTl_Result)
os.exit(0)
vs = {nil}
--v = {1, nil, 3}
v = {}


function  motor_a_dv(s, array)
     n = 0
    
        string.gsub(s , "([^#]*)", function(x)  
                    n= n+1; array[n] = x
                end
                )
    
end 

function  vector_para_motor(vector, limite, valor_defecto)
    valor_defecto = valor_defecto or "" 
    
    
    for i=1, limite do
        if vector[i] == nil or vector[i] == ""  then
            vector[i] = valor_defecto 
        end
    end 
    
    return table.concat(vector, '#', 1, limite)..  '#' 
end 
motor_a_dv("#4#2#3#", v)
for i= 1, #v do
    --print(v[i]..'*')
end
--os.exit(0)

print(vector_para_motor(v, 4, "NULO"))
os.exit(0)
    
function  split(buf, separator, init_pos, limit_pos, out_table)
    
    local n=  1
	
    while true do
        local i = string.find(buf, separator, init_pos, true)
        if  i  and i <= limit_pos  then
            out_table[n] = string.sub(buf, init_pos, i-1)      
            init_pos = i +1
            n = n + 1        
			
        else    
            out_table[n] = string.sub(buf, init_pos, limit_pos)  
            break
        end    
    end   
     
    return n
end 
GDS = {}

function GDS.StringSplit(str, sep) 
    local t = {}
    split(str, sep, 1, #str, t) 
    return t
end

path = [[
C:\Users\macal\Documents\batch\config\?.dll;
C:\Program Files\GDSLink\Dataview360_2.9.0.58\?.lua;
C:\Program Files\GDSLink\Dataview360_2.9.0.58\..\lib\lua\5.3\?.lua;
C:\Program Files\GDSLink\Dataview360_2.9.0.58\loadall.lua;
.\?.lua]]
function search_config_file(path_list, config_name)
    local t = GDS.StringSplit(path_list , ";")
    for j = #t, 1, -1 do
       local question_mark_exists = string.find(t[j], "?", 1, true)
       if not question_mark_exists or string.find(t[j], "%?%..+$") then
           local s
            if question_mark_exists then
               s = string.gsub(t[j], "%?%..+$", "")..config_name
            else
               s = t[j] .."\\"..config_name
            end   
            print("** ", s)
            if exists(s, false) then
                    return s
            end        
       end
    end    
    return nil
end
f = search_config_file(path, "demo_batch.lua")
print(f or "nil")

os.exit(0)

local mapa= {
			CLIENT_IP = "",
            X_FORWARDED_FOR = "",
            X_FORWARDED= "",
            X_CLUSTER_CLIENT_IP= "",
            FORWARDED_FOR= "",
            FORWARDED= "",
            VIA= "",
            REMOTE_ADDR= ""
			}            

function get_host_and_client(header)
    
    local client_priority= { "client_ip",  "x_forwarded_for", "x_forwarded",
                             "x_cluster_client_ip","forwarded_for", "forwarded",
                             "via", "remote_addr"}            

	if not header then return "localhost","localhost"  end
    local t = {}
    for k , v in pairs(header) do
        t[string.lower(k)]= v      
    end    
    
	local host   = t["host"] or "localhost"   
    local client = "localhost"   
    
    for j = 1, #client_priority do
        local clave = client_priority[j]
        if t[clave] and t[clave] ~= "" then
            if clave == "x_forwarded_for" then
                client= (GDS.StringSplit(t[clave], ","))[1]
            else
                client =  t[clave]    
            end            
            break
        end
    end        
  return host, client
end

hh = {HOST = "dd", X_FORWARDED_FOR = "12, 1", CLIENT_IP = "22"}
--hh= {}
for x, y in pairs(hh) do
  --print(x, y)
end
print(get_host_and_client(hh))

os.exit(0)

function leer_sucursales(dir_config)

    if dir_config == nil or dir_config == '' then
        --OutputWarnString("La variable RPX_CONFIG no ha sido establecida o no es correcta")
        --fichero = "/home/reimpex/dataview/dvconfig/sucursales.txt"
        fichero = "C:\\Users\\jose.ruiz\\OneDrive - GDS Link, LLC\\Documents\\Proyectos\\Reimpex\\deploy\\sucursales.txt"
    else
        dir_config= "C:\\Users\\jose.ruiz\\OneDrive - GDS Link, LLC\\Documents\\Proyectos\\Reimpex\\deploy"
        if string.find(dir_config, "/") then
            p = string.find(dir_config, "/$")
        else
            p = string.find(dir_config, "\\[^\\]*$")
        end
        fichero = dir_config:sub(1, p) ..  "sucursales.txt"           
        print("Fichero ", fichero)
    end    
    
  local file= io.open(fichero, "r")  
	if not file then
		print("No se encuentra fichero de scursales " .. fichero)
		return nil
	end
    local map = {}
    local line = ""

    while line ~= nil do
        line = file:read("*l")  
        if line ~= '' and line ~= nil then 
            cod, desc = string.match(line, "([^ ]+)[ ]*\t[ ]*(.+)")
            if cod and desc then
              map[desc] = cod
            end  
        end
    end    
    return map    
end    

map = leer_sucursales("a")
  for k,v in pairs(map) do
    print (k..' '..v)
  end
  