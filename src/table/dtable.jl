import DataFrames
import Tables

import Base: fetch, filter, map, setproperty!, eachrow

mutable struct DTable
    chunks::Vector{Dagger.EagerThunk}
end

function DTable(table::DataFrames.DataFrame; chunksize=10)
    if !Tables.istable(table)
        throw(ArgumentError("Provided input is not Tables.jl compatible."))
    end
    r = Dagger.@spawn Tables.rows(table)
    n = DataFrames.nrow(table)
    partition_rows = (x, l, r) -> getindex(x, l:r)
    df_create = (rows,i) -> DataFrames.DataFrame( partition_rows(rows, i, (i + chunksize - 1) % (n + 1)))
    return DTable([Dagger.@spawn df_create(r,i) for i in 1:chunksize:n])
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
