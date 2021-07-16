using Test
using DataFrames

@testset "dtable" begin

    @testset "constructors" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; thunksize=100)
        @test fetch(d) == s
    end

    @testset "map" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; thunksize=100)
        @test map(x-> x.a + x.b, eachrow(s)) == fetch(map(x-> x.a + x.b, d))
    end

    @testset "filter" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; thunksize=100)
        s_r = map(x-> x.a + x.b, eachrow(filter(x-> x.a > 0.5, s)))
        d_r = fetch(map(x-> x.a + x.b, filter(x-> x.a > 0.5, d)))
        @test s_r == d_r
    end
end