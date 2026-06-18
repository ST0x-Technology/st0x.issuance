#!/usr/bin/env nu

# Smoke-tests an already-built issuance-bot image by running the server binary
# the way production does (compose runs `./server`) and asserting it actually
# starts. With no config the binary loads, reaches clap, and exits on the
# missing required RPC_URL, printing usage that names `st0x-issuance`. That
# marker proves the binary executed; anything that stops it from running fails
# to produce it, so the captured output is printed for diagnosis.

def main [image: string] {
  let result = (^timeout 30 docker run --rm $image ./server | complete)
  let output = $"($result.stdout)($result.stderr)"

  print $output

  if not ($output | str contains "st0x-issuance") {
    print "::error::server binary did not start"
    exit 1
  }

  print "server binary started"
}
