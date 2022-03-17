@0xb7009dd0fed1ac1c;

struct Config {
    struct Entry {
        name        @0 :Text;
        sturdyRef   @1 :Text;
    }
    entries @0 :List(Entry);
}
