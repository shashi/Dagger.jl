using Test
using DataFrames
using Arrow
using CSV

@testset "dtable" begin

    @testset "constructors - Tables.jl compatibility (Vector{NamedTuple})" begin
        size = 1_000
        d = DTable([(a=10, b=20) for i in 1:size]; chunksize=100)
        @test fetch(d) == DataFrame([(a=10, b=20) for i in 1:size])
    end

    @testset "constructors - Tables.jl compatibility (DataFrames)" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; chunksize=100)
        @test fetch(d) == s
    end

    @testset "constructors - Tables.jl compatibility (Arrow)" begin
        size = 1_000
        io = IOBuffer()
        Arrow.write(io, [(a=10, b=20) for i in 1:size])
        t = Arrow.Table(take!(io))
        d = DTable(t; chunksize=100)
        @test fetch(d) == DataFrame(t)
    end

    @testset "constructors - Tables.jl compatibility (CSV)" begin
        size = 15_000
        io = IOBuffer()
        CSV.write(io, [(a=10, b=20) for i in 1:size])
        d = CSV.read(take!(io), DTable) # TODO: figure out how to add a keyword arg here (for now default value in constructor)
        CSV.write(io, [(a=10, b=20) for i in 1:size])
        # or
        d2 = DTable(CSV.File(take!(io)); chunksize=1_000)
        @test fetch(d) == DataFrame([(a=10, b=20) for i in 1:size])
        @test fetch(d2) == DataFrame([(a=10, b=20) for i in 1:size])
    end

    @testset "map" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; chunksize=100)
        @test map(x-> x.a + x.b, eachrow(s)) == fetch(map(x-> x.a + x.b, d))
    end

    @testset "filter" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; chunksize=100)
        s_r = map(x-> x.a + x.b, eachrow(filter(x-> x.a > 0.5, s)))
        d_r = fetch(map(x-> x.a + x.b, filter(x-> x.a > 0.5, d)))
        @test s_r == d_r
    end
end