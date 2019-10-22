{
    "targets": [{
        "target_name": "kudujs",
        "cflags": [ "-std=c++17" ],
        "cflags!": [ "-fno-exceptions" ],
        "cflags_cc!": [ "-fno-exceptions", "-fno-rtti" ],
        "sources": [
            "cppsrc/main.cpp",
            "cppsrc/kudunode.cpp",
            "cppsrc/kuduclass.cpp",
            "cppsrc/kudujs.cpp"
        ],
        "link_settings": {
          "libraries": [
            "-lkudu_client"
          ]
        },
        'include_dirs': [
            "<!@(node -p \"require('node-addon-api').include\")"
        ],
        'libraries': [],
        'dependencies': [
            "<!(node -p \"require('node-addon-api').gyp\")"
        ],
        'defines': [ 'NAPI_DISABLE_CPP_EXCEPTIONS' ]
    }]
}