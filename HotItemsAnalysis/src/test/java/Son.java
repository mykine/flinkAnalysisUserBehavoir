class Son extends Father
{
    static
    {
        System.out.println("儿子在静态代码块");
    }

    public Son()
    {
        System.out.println("儿子的构造方法：我是儿子~");
    }

    static void say(){
        System.out.println("儿子 say  hello");
    }
}