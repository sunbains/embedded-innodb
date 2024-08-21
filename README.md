# Embedded InnoDB 0.1
This is the source of Embedded InnoDB 0.1

The long term plan is to convert it incrementally to Zig. Starting with the tests.

So far only lightly tested on Ubuntu 23.10.

Instructions for compiling and installing, setup to compile with C++ 23.

1. cmake -G Ninja -DCMAKE_BUILD_TYPE=(debug|Release) .

2. ninja

3. #ninja install (not tested yet)

4. Enjoy!

Resources:
https://nivethan.dev/devlog/extending-a-c-project-with-zig.html
https://zig.guide/working-with-c/abi/

## Contribution Guidelines

### C++ Code Style 
1. All iterators should be named, `it`. Example:
    ```
    for(auto it : map.find(key); it != map.end()){
        ...
    }
    ```
   If there is a possibility for name collision, bound the scope. Example: 
   ```
   {
        auto it = name_lookup.find(key);
        ...
   }
   
   <awesome code here>
   
   {
        auto it = id_lookup.find(id);
        ...
   }
   ```

2. Class/Struct names should be upper case first letter, and then snake case. Example: 
    ```
    struct Table_dict_data;
    struct Buf_pool_manager;
   ```

3. All internal struct members should be prefixed with `m_`; Example: 
   ```
   struct Table_dict_data{
    std::string m_name; 
    std::uint64_t m_id;
    ....
   };
   ```
4. Tag the functions appropriately with `const`, `[[nodiscard]]`, and `noexcept` if required. Example: 
   ```
   [[nodiscard]] bool enqueue(T const &data) noexcept 
   ```
5. More coming soon!!