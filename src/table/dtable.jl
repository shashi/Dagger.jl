import DataFrames
import Tables

import Base: fetch, filter, map

struct DTable
    chunks::Vector{Dagger.EagerThunk}

    DTable(chunks::Vector{Dagger.EagerThunk}) = new(chunks)
end

function DTable(table; chunksize=10_000)
    if !Tables.istable(table)
        throw(ArgumentError("Provided input is not Tables.jl compatible."))
    end

    create_chunk = (rows) -> Dagger.@spawn DataFrames.DataFrame(deepcopy(rows))
    chunks = Vector{Dagger.EagerThunk}()

    it = Tables.rows(table)
    buffer = Vector{eltype(it)}()

    p = iterate(it)
    counter = 0

    while !isnothing(p) 
        push!(buffer, p[1])
        counter += 1
        p = iterate(it, p[2])
        if counter == chunksize
            push!(chunks, create_chunk(buffer))
            empty!(buffer)
            counter = 0 
        end
    end
    if counter > 0
        push!(chunks, create_chunk(buffer))
        empty!(buffer)
    end
    return DTable(chunks)
end

function filter(f, d::DTable)
    _f = x -> Dagger.@spawn filter(f, x)
    DTable(map(_f, d.chunks))
end

function fetch(d::DTable)
    _fetch_thunk_vector(d.chunks)
end

function _fetch_thunk_vector(x)
    vcat(fetch.(x)...)
end

function fetchcolumn(d::DTable, s::Symbol)
    _f = (x) -> Dagger.@spawn getindex(x, :, s)
    _fetch_thunk_vector(map(_f, d.chunks))
end

function map(f, d::DTable)
    thunk_f = x -> Dagger.@spawn eachrow(x) # eachrow baked in here for now
    row_f = x -> Dagger.@spawn map(f, thunk_f(x))
    DTable(map(row_f, d.chunks))
end

export DTable
