#!/bin/sh
set -eu

# This VM supplies its own kernel. Docker Desktop's LinuxKit kernel lacks the
# ext4 quota features required by the release profile.
test "$(uname -s):$(uname -m)" = Darwin:arm64 || {
    echo 'This fixture requires Apple Silicon macOS and QEMU.' >&2
    exit 1
}
root="$PWD/.local/quota-vm"
image=debian-13-genericcloud-arm64-20261001-2618.qcow2
checksum=d8470b8c6c38fead046c794b5800a5a7b96672d5bcf543cc230ceb0c4b8ace05ed341a0c8928045422243fde26b2f1f2f58e99244c65709ffda2e3d4b674dd5a
port=${DISPATCH_QUOTA_VM_PORT:-22231}
case "$port" in ''|*[!0-9]*) exit 1 ;; esac
test "$port" -ge 1024 && test "$port" -le 65535
mkdir -p "$root/seed"
chmod 700 "$root" "$root/seed"
if ! test -f "$root/$image"; then
    curl --fail --location --retry 3 --silent --show-error \
        "https://cloud.debian.org/images/cloud/trixie/20261001-2618/$image" \
        -o "$root/$image.pending"
    printf '%s  %s\n' "$checksum" "$root/$image.pending" | shasum -a 512 -c
    mv "$root/$image.pending" "$root/$image"
fi
printf '%s  %s\n' "$checksum" "$root/$image" | shasum -a 512 -c
if ! test -f "$root/id_ed25519"; then
    ssh-keygen -q -t ed25519 -N '' -C dispatch-quota-fixture -f "$root/id_ed25519"
fi
key=$(cat "$root/id_ed25519.pub")
cat > "$root/seed/user-data" <<EOF
#cloud-config
users:
  - name: dispatch
    shell: /bin/bash
    lock_passwd: true
    sudo: ALL=(ALL) NOPASSWD:ALL
    ssh_authorized_keys:
      - $key
ssh_pwauth: false
disable_root: true
EOF
printf 'instance-id: dispatch-quota-v1\nlocal-hostname: dispatch-quota\n' > "$root/seed/meta-data"
if ! test -f "$root/seed.iso"; then
    hdiutil makehybrid -iso -joliet -default-volume-name cidata \
        -o "$root/seed.iso" "$root/seed" >/dev/null
fi
if ! test -f "$root/disk.qcow2"; then
    qemu-img create -f qcow2 -F qcow2 -b "$root/$image" "$root/disk.qcow2" 4G
fi
if ! test -f "$root/vars.fd"; then
    cp /opt/homebrew/share/qemu/edk2-arm-vars.fd "$root/vars.fd"
fi
# Keep the emulator in the foreground so its tool session is authoritative.
# SSH is exposed only on loopback and uses this fixture's disposable key.
exec qemu-system-aarch64 -machine virt -accel hvf -cpu host -smp 2 -m 1024 \
    -drive if=pflash,format=raw,readonly=on,file=/opt/homebrew/share/qemu/edk2-aarch64-code.fd \
    -drive if=pflash,format=raw,file="$root/vars.fd" \
    -drive if=virtio,format=qcow2,file="$root/disk.qcow2" \
    -drive if=virtio,format=raw,readonly=on,file="$root/seed.iso" \
    -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:$port-:22" \
    -device virtio-net-pci,netdev=net0 -display none \
    -serial "file:$root/serial.log" -monitor none -no-reboot
