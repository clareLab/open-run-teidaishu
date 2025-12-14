#!/usr/bin/env bash
set -euo pipefail

yaml_get() {
  local file="$1"
  local target="$2"

  awk -v target="$target" '
    function trim(s){ sub(/^[ \t]+/,"",s); sub(/[ \t]+$/,"",s); return s }
    function unquote(s){
      s=trim(s)
      gsub(/^"/,"",s); gsub(/"$/,"",s)
      gsub(/^'\''/,"",s); gsub(/'\''$/,"",s)
      return s
    }
    function build_path(level,  i, p){
      p=stack[0]
      for(i=1;i<=level;i++){
        if(stack[i]!="") p=p "." stack[i]
      }
      return p
    }
    BEGIN{ for(i=0;i<64;i++) stack[i]="" }
    {
      line=$0
      sub(/\r$/,"",line)
      if(line ~ /^[[:space:]]*#/ || line ~ /^[[:space:]]*$/) next

      m = match(line, /[^ ]/)
      indent = (m>0 ? m-1 : 0)
      level = int(indent/2)

      if(line ~ /^[[:space:]]*-[[:space:]]+/) next

      if(match(line, /^[[:space:]]*[^:#-][^:]*:/)){
        key=line
        sub(/:.*/, "", key)
        key=trim(key)

        rest=line
        sub(/^[[:space:]]*[^:]*:/, "", rest)
        rest=trim(rest)

        stack[level]=key
        for(i=level+1;i<64;i++) stack[i]=""

        path=build_path(level)

        if(rest!="" && path==target){
          print unquote(rest)
          exit 0
        }

        if(rest=="" && path==target){
          print ""
          exit 0
        }
      }
    }
  ' "$file"
}

yaml_list() {
  local file="$1"
  local target="$2"

  awk -v target="$target" '
    function trim(s){ sub(/^[ \t]+/,"",s); sub(/[ \t]+$/,"",s); return s }
    function unquote(s){
      s=trim(s)
      gsub(/^"/,"",s); gsub(/"$/,"",s)
      gsub(/^'\''/,"",s); gsub(/'\''$/,"",s)
      return s
    }
    function build_path(level,  i, p){
      p=stack[0]
      for(i=1;i<=level;i++){
        if(stack[i]!="") p=p "." stack[i]
      }
      return p
    }
    BEGIN{
      inside=0
      inside_level=0
      for(i=0;i<64;i++) stack[i]=""
    }
    {
      line=$0
      sub(/\r$/,"",line)
      if(line ~ /^[[:space:]]*#/ || line ~ /^[[:space:]]*$/) next

      m = match(line, /[^ ]/)
      indent = (m>0 ? m-1 : 0)
      level = int(indent/2)

      if(match(line, /^[[:space:]]*[^:#-][^:]*:/)){
        key=line
        sub(/:.*/, "", key)
        key=trim(key)

        rest=line
        sub(/^[[:space:]]*[^:]*:/, "", rest)
        rest=trim(rest)

        stack[level]=key
        for(i=level+1;i<64;i++) stack[i]=""

        path=build_path(level)

        if(rest=="" && path==target){
          inside=1
          inside_level=level
          next
        }

        if(inside && level<=inside_level){
          inside=0
        }

        next
      }

      if(inside && line ~ /^[[:space:]]*-[[:space:]]+/){
        v=line
        sub(/^[[:space:]]*-[[:space:]]+/, "", v)
        v=unquote(v)
        if(v!="") print v
      }
    }
  ' "$file"
}
