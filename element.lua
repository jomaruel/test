--[[ =============================================================================================
	 batch.lua
	 
	 Module to be "required" in DV to turn an on-line sequence into a batch sequence
	 
	 (c) GDS-Modellica 
	 initial version by JM Ruiz 2023
     =============================================================================================--]]

local values  = {"1", "2.0", "3", "4", "x"}
local input_headers = { "score", "af11", "af12" , "af1", "desco"}
local headers = { score = 1, f11= 2, f12=3 , f1=4, desco=5}

input.request.n1.n2[1].innervalue = 1
input.request.n1.n2[2].innervalue = 2

local EMPTY_TABLE = {} -- read-only

MSG_PREFIX = "** Batch: "

function message(msg, ...) return #{...} ~= 0 and ((MSG_PREFIX or '')..msg):format(table.unpack{...}) or ((MSG_PREFIX or '')..msg)  end

function dv_err(msg, ...) OutputErrorString(message(msg, ...))  end
function dv_info(msg, ...) OutputInfoString(message(msg, ...))  end
function dv_warn(msg, ...) OutputWarnString(message(msg, ...))  end


function split2(s, delim, pos_ini, pos_end)
	return GDS.StringSplit(string.char(string.byte(s, pos_ini, pos_end)), delim)	
end


--[[ =============================================================================================
	 LUA tonumber() uses the locale when converting to number so it must be previouly set before
	 calling convert_to_num()
 =============================================================================================--]]

function convert_to_num(string_value)
	put("**", string_value, tonumber(string_value))
	return tonumber(string_value) or 0/0  -- return NaN is conversion  is not posiible
end
function convert_to_string(num_value)
	if not num_value or num_value ~= num_value then
		return  config.nan_text
	end	
	return string.format(config.out_fmt, num_value)
end


local output_field_info = {}
local input_field_info = {}
DVindex_2_FileIndex = {}	

--[[ =============================================================================================
	Recursively scans gds record starting at node whose id and table are node_id and table_node
	Called twice on init() (one for input record, other for ouput record)
	
	Builds the out parameter field_info (not returned as function result due to recursion)	
    =============================================================================================--]]
	

function get_fields_info(node_id, table_node, header_fields, field_info)

	local node = GDS.GetNode(node_id)		
	
	
	
	for k, v in ipairs(node.children) do
        
        local element, prop
        local element_is_field
        if v.type == "field" then
            element = GDS.GetField(v.id)
            element_is_field = true
        else
            element= GDS.GetNode(v.id)
            element_is_field = false
        end
        prop =  element.properties 
        
        if element_is_field or (prop.occurrenceType =="Multiple" and prop.jsonNodeType == "Array") then
            local index
            if  header_fields == nil then
                index = 0
            else
                index = header_fields[prop.displayName] 
                if index == nil then index = -1 end
            end
            if index >=0 then            				
                local n = #field_info + 1				
                DVindex_2_FileIndex[n] = index   
				
				field_info[n] = {}             
                local p =  field_info[n]
                if element_is_field then
                    p.table_node = table_node
                    p.key = prop.name
					if prop.fieldtype ~= 'numeric' then
						p.func= nil 
					else
						p.func= convert_to_num
					end	
                    p.single =  true
                else
					local children = (element.children or {})[1]
					if children then
	                    p.single =  false
	                    p.table_node = table_node[v.name]                        
	                    p.key = children.name
	                 	if prop.fieldtype ~= 'numeric' then
							p.func= nil 
						else
							p.func= convert_to_num
						end	
					end	
                end            
            end  
        elseif element_is_field == false then
            get_fields_info(v.id, table_node[v.name], header_fields, field_info)
        end    
	end
end	

function populate(fields_values, field_info)	
	for j = 1, #field_info do
		local p = field_info[j]		
		if p.table_node then
			p.table_node[p.key] = p.func == nil and fields_values[j]  or p.func(fields_values[j])
		end
	end
end	
function populate2(fields_values, field_info)	
	for j = 1, #field_info do
		local idx_field =  DVindex_2_FileIndex[j]		
		local p = field_info[j]	
		if idx_field then
			p.table_node[p.key] = p.func == nil and fields_values[idx_field]  or p.func(fields_values[idx_field])
		end
	end
end	

function dump(fields_values, field_info)
	for j = 1, #field_info do
		local p = field_info[j]		
		if p.table_node then
			fields_values[j] = p.table_node[p.key]
		end
	end
end	

--[[=============================================================================================
	 get root nodes of this listener_id
    =============================================================================================--]]
function root_nodes(listener_id)
	local l = GDS.GetListeners()	
	for k, v in pairs (l) do			
		if k == listener_id then
			return v.input[1], v.output[1]  
		end
	end		
	return nil, nil
end	
--[[=============================================================================================
	 get locale this listener_id
    =============================================================================================--]]
function get_locale(listener_id)
	local l = GDS.GetListeners()	
	for k, v in pairs (l) do			
		if k == listener_id then
			return v.input[1], v.output[1]  -- could be nil
		end
	end		
	return nil, nil
end	

config= {}
config["listener_id"] = 24
config.nan_text ="NuN"
config.out_fmt  ="%.9f"
config.locale  ="ES-es"


OutputDebugString(os.setlocale('ES_es', "numeric"))

--put(string.format("%.10f", input.request.n1.f11))
--input.request.n1.f11 = math.sqrt(1/0)
--put(string.format("%s", input.request.n1.f11))
--OutputDebugString(input.request.n1.f11)


function DV_init()

--[[=============================================================================================
	 set locale
    =============================================================================================--]]
	if os.setlocale(config.locale) ~= config.locale then
		dv_warn("Couldn't set locale to '%s'. The systmem will continue with locale '%s'", config.locale, os.setlocale())
	end	
--[[=============================================================================================
	 get root nodes for the listener
    =============================================================================================--]]	
	local input_node, output_node = root_nodes(config.listener_id)
	
	
	if not input_node then
		dv_err("Input node not found for listener %d", config.listener_id)
	else
		get_fields_info(input_node.id, input[input_node.name], headers, input_field_info)		
		populate2(values, input_field_info)		
	end
	if not output_node then
		dv_err("Output node not found for listener %d", config.listener_id)
	else
		get_fields_info(output_node.id, input[output_node.name], nil, output_field_info)	
	end
	
end	
 
 DV_init()



